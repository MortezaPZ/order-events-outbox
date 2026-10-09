# Order Events Outbox

An order service that does not lose an event when the broker is down, does not apply a duplicate delivery twice, and does not block the queue on a poison message.

## Overview

The order and its event are written in one database transaction. A relay publishes the outbox later. Consumers claim each event id once. Failures retry with a bound, then land in a dead-letter list that can be replayed.

## Features

- Transactional outbox
- Idempotent consumers via an atomic `INSERT` claim
- Bounded retry and a dead-letter queue
- Permanent errors skip the retry ladder
- Unknown event types are acknowledged, not retried forever
- Events carry a schema version
- In-memory, flaky, and Redis brokers behind one protocol

## Technology Stack

- Python 3.11+
- FastAPI
- SQLite
- Optional Redis
- Docker

## Architecture

Publishing inside the request leaves a window: the order can commit and the process can die before the broker accepts the message. Writing both rows in one transaction closes that window. Delivery is at-least-once, so the consumer must be idempotent.

The claim is one insert. A select-then-insert would let two workers pass the check. A failed handler releases its claim. If it did not, the retry would look like a duplicate and the event would disappear.

The relay stops at the first publish failure. Skipping ahead would deliver later events first. Everything behind the failure stays pending.

`/health` reports `outbox_pending` and `queue_depth` separately. A growing outbox means the relay is stuck. A growing queue means the workers are behind.

## Installation

```bash
python -m venv .venv
```

Windows: `.venv\Scripts\activate`. Linux or macOS: `source .venv/bin/activate`.

```bash
pip install -r requirements.txt
python demo.py
uvicorn orderflow.api:app --reload
```

Docker:

```bash
docker compose up --build
BROKER=redis docker compose up
```

## Usage

`python demo.py` runs five scenarios: a normal order, a broker outage that still delivers, duplicate delivery applied once, a poison message that is dead-lettered and replayed, and 500 orders with nothing left in the outbox.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Order count, outbox depth, queue depth, dead letters |
| `POST` | `/orders` | Place an order and write the event atomically |
| `GET` | `/orders`, `/orders/{id}` | Read orders |
| `POST` | `/orders/{id}/pay` | Emit `order.paid` |
| `POST` | `/orders/{id}/cancel` | Emit `order.cancelled` |
| `GET` | `/dead-letters` | Inspect failures |
| `POST` | `/dead-letters/{id}/replay` | Requeue after the cause is fixed |

```bash
curl -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer":"Acme","amount":249.99}'
```

## Testing

```bash
pytest tests -q
```

53 tests cover envelopes, outbox atomicity, the relay, the eight-thread claim race, retry math, dead letters, and the API.

CI on Python 3.11, 3.12, and 3.13 lints, tests, builds the image, and smoke-tests the running container: it waits for `/health`, places an order, and checks that the order reaches `reserved`.

## Limitations

The default broker is in-process. Redis is optional. This is the outbox pattern for one order service, not a general message platform.

## License

MIT
