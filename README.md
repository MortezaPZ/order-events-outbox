# Order Events

An event-driven order service built around the three failure modes that break
naive message-based systems:

| Failure | Answer here |
|---|---|
| DB commit succeeds, publish fails → event lost | **Transactional outbox** |
| Message delivered twice → side effect runs twice | **Idempotent consumers** |
| Message always fails → queue blocked forever | **Bounded retry + dead-letter** |

**FastAPI · SQLite · pluggable broker · Docker · GitHub Actions.** Runs with no
infrastructure: `python demo.py` and it works.

---

## How it flows

```
POST /orders ──┬──▶ orders table     ┐
               └──▶ outbox table     ┘  ONE transaction
                        │
                  relay (poller)  ──── broker down? row stays pending
                        │
                        ▼
                  broker (memory | redis)
                        │
                  worker — claim(event_id)
                   │            │
              first time     duplicate → skip
                   │
              handler runs
                   │
            ┌──────┴──────┐
          success        raises
                           │
                  attempt < max? ──yes──▶ requeue with backoff
                           │
                          no ──▶ dead_letters (replayable)
```

---

## The five guarantees, demonstrated

`python demo.py` runs each one and prints what happened:

```
1. Normal flow
  placed          order=1  outbox_pending=1
  relay published queue=1  outbox_pending=0
  worker done     status=reserved
  after payment   status=confirmed

2. Broker outage — the outbox holds the event until delivery works
  pass 1: BROKER DOWN  outbox_pending=1
  pass 2: BROKER DOWN  outbox_pending=1
  pass 3: delivered    outbox_pending=0
  nothing lost    status=reserved

3. Duplicate delivery — at-least-once, but the effect runs once
  delivery 1: processed=1 duplicates_skipped=0
  delivery 2: processed=1 duplicates_skipped=1
  delivery 3: processed=1 duplicates_skipped=2

4. Poison message — bounded retries, then dead-letter, then replay
  attempt 1: retried=1 dead_lettered=0
  attempt 2: retried=2 dead_lettered=0
  attempt 3: retried=2 dead_lettered=1
  after replay:   status=reserved  dead_letters=0

5. Bulk flow — 500 orders, nothing lost
  orders placed:  500
  events relayed: 500
  reserved:       500
  outbox left:    0
  dead letters:   0
```

Scenario 2 is the one worth reading twice: the broker was down for two passes
and the event was still delivered, because it was never only in the queue.

---

## Quick start

```bash
python -m venv .venv
.venv/Scripts/activate            # source .venv/bin/activate on Linux/macOS
pip install -r requirements.txt

python demo.py                              # the five scenarios above
pytest tests -q                             # 53 tests
uvicorn orderflow.api:app --reload          # API on http://localhost:8000
```

With Docker:

```bash
docker compose up --build           # API + Redis
BROKER=redis docker compose up      # use the real queue
```

---

## API

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/health` | Orders, outbox depth, queue depth, dead letters |
| `POST` | `/orders` | Place an order — writes order + event atomically |
| `GET` | `/orders` · `/orders/{id}` | Read orders |
| `POST` | `/orders/{id}/pay` | Emit `order.paid` |
| `POST` | `/orders/{id}/cancel` | Emit `order.cancelled` |
| `GET` | `/dead-letters` | Inspect what failed and why |
| `POST` | `/dead-letters/{id}/replay` | Requeue after fixing the cause |

```bash
curl -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer":"Acme","amount":249.99}'

curl http://localhost:8000/health
```

`/health` exposes `outbox_pending` and `queue_depth` on purpose: a growing
outbox means the relay is stuck, a growing queue means workers are behind, and
those are different incidents.

---

## Design decisions worth explaining

**Why an outbox instead of publishing inside the request?** Because "write to
the DB, then publish" has a window: if the process dies between the two, the
order exists and nobody hears about it. Writing both in one transaction removes
that window. The cost is at-least-once delivery instead of exactly-once — which
is why consumers are idempotent.

**The claim is one atomic statement.** `INSERT INTO processed_events` either
succeeds or hits the primary key. A check-then-act (`SELECT` then `INSERT`)
would let two workers both pass the check on a redelivery. A test races eight
threads at the same event id and asserts exactly one wins.

**A failed handler releases its claim.** This is the subtle one: if the claim
leaked, the retry would be dismissed as a duplicate and the event would vanish
silently — worse than the original failure. There is a test for exactly that.

**The relay stops at the first publish failure.** It does not skip ahead to the
next event, because that would deliver events out of order. Everything behind
the failure stays pending and is retried on the next pass.

**Permanent vs transient failures are different.** A handler raises
`PermanentError` for a payload that will never succeed (a missing field), and it
dead-letters immediately instead of burning three attempts on it.

**Unknown event types are acknowledged, not requeued.** In a real fleet another
service may be the intended consumer. Requeuing would spin forever.

**Events carry a schema version.** A consumer that receives a payload newer than
it understands refuses it rather than misreading it — which matters during a
rolling deploy where old and new consumers run side by side.

**Dead letters are replayable.** Losing a message to a bug you have since fixed
is not acceptable; `POST /dead-letters/{id}/replay` puts it back with a reset
attempt counter.

---

## Tests — 53

| Area | Covers |
|---|---|
| Event envelope | round-trip, validation, malformed JSON, future schema |
| Outbox atomicity | order+event together, no orphan on rejection, ordering |
| Relay | delivery, broker outage, order preservation on failure |
| Idempotency | claim once, 8-thread race, release/retake, redelivery |
| Retry & DLQ | backoff maths, requeue with attempt, dead-letter, replay |
| Claim release | a failed handler must not block its own retry |
| End-to-end | full lifecycle, 40 orders with no loss, poison handling |
| API | placement, transitions, validation, dead-letter operations |

---

## CI

`.github/workflows/ci.yml` runs lint and tests on Python 3.11/3.12/3.13, then
builds the Docker image and **smoke-tests the running container** — waits for
`/health`, places a real order, and asserts it reaches `reserved`. An image that
builds but does not serve is not a passing build.

---

## Layout

```
order-events/
├── orderflow/
│   ├── events.py      # envelope, versioning, serialisation
│   ├── store.py       # orders, outbox, claims, dead letters (SQLite)
│   ├── broker.py      # Broker protocol: memory | flaky | redis
│   ├── relay.py       # outbox → broker
│   ├── worker.py      # idempotency, retry ladder, dead-lettering
│   ├── handlers.py    # business logic
│   └── api.py         # FastAPI
├── tests/test_orderflow.py
├── .github/workflows/ci.yml
├── Dockerfile · docker-compose.yml
└── demo.py
```

## License

MIT
