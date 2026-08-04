import threading
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from orderflow.api import Services, create_app
from orderflow.broker import (
    BrokerError,
    FlakyBroker,
    InMemoryBroker,
    resolve_broker,
)
from orderflow.events import (
    ORDER_CANCELLED,
    ORDER_PAID,
    ORDER_PLACED,
    SCHEMA_VERSION,
    Event,
    EventError,
)
from orderflow.handlers import register
from orderflow.relay import OutboxRelay
from orderflow.store import Order, Store, StoreError
from orderflow.worker import PermanentError, RetryPolicy, Worker


def make_order(order_id: str = 'ORD-1', amount: float = 99.0) -> Order:
    return Order(
        id=order_id,
        customer='Acme Ltd',
        amount=amount,
        currency='GBP',
        status='placed',
        created_at=datetime.now(UTC).isoformat(),
    )


def placed_event(order_id: str = 'ORD-1') -> Event:
    return Event(type=ORDER_PLACED, payload={'order_id': order_id})


@pytest.fixture
def store():
    s = Store()
    yield s
    s.close()


@pytest.fixture
def broker():
    return InMemoryBroker()


@pytest.fixture
def pipeline(store, broker):
    """Store + relay + worker, driven manually so tests stay deterministic."""
    relay = OutboxRelay(store, broker)
    worker = register(Worker(store, broker, sleep=lambda _: None), store)
    return store, broker, relay, worker


class TestEvent:
    def test_round_trips_through_json(self):
        event = placed_event()
        restored = Event.from_json(event.to_json())

        assert restored.id == event.id
        assert restored.type == event.type
        assert restored.payload == event.payload

    def test_ids_are_unique(self):
        assert placed_event().id != placed_event().id

    def test_empty_type_is_rejected(self):
        with pytest.raises(EventError):
            Event(type='', payload={})

    def test_non_dict_payload_is_rejected(self):
        with pytest.raises(EventError):
            Event(type='x', payload=['not', 'a', 'dict'])

    def test_malformed_json_is_reported(self):
        with pytest.raises(EventError, match='not valid JSON'):
            Event.from_json('{nope')

    def test_missing_fields_are_named(self):
        with pytest.raises(EventError, match='missing fields'):
            Event.from_json('{"id": "1"}')

    def test_newer_schema_is_refused(self):
        # Reading a v2 payload with v1 code would silently misinterpret it.
        future = SCHEMA_VERSION + 1
        raw = f'{{"id":"1","type":"x","payload":{{}},"version":{future}}}'
        with pytest.raises(EventError, match='newer'):
            Event.from_json(raw)

    def test_with_attempt_preserves_identity(self):
        event = placed_event()
        retried = event.with_attempt(3)

        assert retried.id == event.id
        assert retried.attempt == 3
        assert event.attempt == 1


class TestOutboxAtomicity:
    def test_order_and_event_are_written_together(self, store):
        store.place_order(make_order(), placed_event())

        assert store.count_orders() == 1
        assert store.outbox_depth() == 1

    def test_duplicate_order_writes_nothing(self, store):
        store.place_order(make_order('ORD-1'), placed_event('ORD-1'))

        with pytest.raises(StoreError, match='already exists'):
            store.place_order(make_order('ORD-1'), placed_event('ORD-1'))

        # The rejected attempt must not have left an orphan outbox row.
        assert store.count_orders() == 1
        assert store.outbox_depth() == 1

    def test_pending_outbox_is_ordered(self, store):
        for i in range(3):
            store.place_order(make_order(f'ORD-{i}'), placed_event(f'ORD-{i}'))

        pending = store.pending_outbox()
        assert [e.payload['order_id'] for e in pending] == ['ORD-0', 'ORD-1', 'ORD-2']

    def test_marking_published_removes_from_pending(self, store):
        store.place_order(make_order(), placed_event())
        event = store.pending_outbox()[0]

        assert store.mark_published([event.id]) == 1
        assert store.outbox_depth() == 0

    def test_marking_twice_is_a_no_op(self, store):
        store.place_order(make_order(), placed_event())
        event = store.pending_outbox()[0]

        store.mark_published([event.id])
        assert store.mark_published([event.id]) == 0


class TestRelay:
    def test_relay_publishes_pending_events(self, pipeline):
        store, broker, relay, _ = pipeline
        store.place_order(make_order(), placed_event())

        assert relay.drain_once() == 1
        assert broker.depth() == 1
        assert store.outbox_depth() == 0

    def test_broker_outage_leaves_the_event_pending(self, store):
        """The whole point of the outbox: an outage delays, never loses."""
        flaky = FlakyBroker(fail_times=1)
        relay = OutboxRelay(store, flaky)
        store.place_order(make_order(), placed_event())

        assert relay.drain_once() == 0
        assert store.outbox_depth() == 1  # still recorded, not lost
        assert relay.stats.failed == 1

        # Broker recovers; the next pass delivers it.
        assert relay.drain_once() == 1
        assert store.outbox_depth() == 0
        assert flaky.depth() == 1

    def test_relay_stops_at_the_first_failure_to_keep_order(self, store):
        flaky = FlakyBroker(fail_times=0)
        relay = OutboxRelay(store, flaky)

        for i in range(3):
            store.place_order(make_order(f'ORD-{i}'), placed_event(f'ORD-{i}'))

        flaky.fail_times = 1
        flaky.attempts = 0

        relay.drain_once()
        # First publish failed, so nothing after it was attempted.
        assert store.outbox_depth() == 3

    def test_drain_empties_the_outbox(self, pipeline):
        store, broker, relay, _ = pipeline
        for i in range(5):
            store.place_order(make_order(f'ORD-{i}'), placed_event(f'ORD-{i}'))

        assert relay.drain() == 5
        assert store.outbox_depth() == 0
        assert relay.stats.published == 5

    def test_drain_on_empty_outbox_does_nothing(self, pipeline):
        _, _, relay, _ = pipeline
        assert relay.drain() == 0


class TestIdempotency:
    def test_claim_succeeds_once(self, store):
        assert store.claim_event('evt-1', 'worker') is True
        assert store.claim_event('evt-1', 'worker') is False

    def test_different_consumers_claim_independently(self, store):
        assert store.claim_event('evt-1', 'billing') is True
        assert store.claim_event('evt-1', 'shipping') is True

    def test_released_claim_can_be_retaken(self, store):
        store.claim_event('evt-1', 'worker')
        store.release_claim('evt-1', 'worker')
        assert store.claim_event('evt-1', 'worker') is True

    def test_concurrent_claims_produce_exactly_one_winner(self, store):
        wins = []
        barrier = threading.Barrier(8)

        def attempt():
            barrier.wait()
            if store.claim_event('evt-hot', 'worker'):
                wins.append(1)

        threads = [threading.Thread(target=attempt) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(wins) == 1

    def test_redelivered_event_runs_the_handler_once(self, pipeline):
        store, broker, relay, worker = pipeline
        calls = []

        worker._handlers[ORDER_PLACED] = lambda e: calls.append(e.id)

        store.place_order(make_order(), placed_event())
        relay.drain()
        event = broker.consume()

        worker.handle(event)
        worker.handle(event)  # at-least-once delivery means this happens

        assert len(calls) == 1
        assert worker.stats.duplicates == 1


class TestRetryAndDeadLetter:
    def _worker(self, store, broker, max_attempts=3):
        return Worker(
            store, broker, retry=RetryPolicy(max_attempts=max_attempts),
            sleep=lambda _: None,
        )

    def test_transient_failure_is_requeued_with_a_higher_attempt(self, store, broker):
        worker = self._worker(store, broker)
        worker._handlers['x'] = lambda e: (_ for _ in ()).throw(RuntimeError('boom'))

        worker.handle(Event(type='x', payload={}))

        requeued = broker.consume()
        assert requeued is not None
        assert requeued.attempt == 2
        assert worker.stats.retried == 1

    def test_event_dead_letters_after_max_attempts(self, store, broker):
        worker = self._worker(store, broker, max_attempts=3)
        worker._handlers['x'] = lambda e: (_ for _ in ()).throw(RuntimeError('boom'))

        worker.handle(Event(type='x', payload={}, attempt=3))

        letters = store.dead_letters()
        assert len(letters) == 1
        assert letters[0]['attempts'] == 3
        assert broker.depth() == 0  # not requeued — it would block the queue

    def test_permanent_error_skips_the_retry_ladder(self, store, broker):
        worker = self._worker(store, broker, max_attempts=5)
        worker._handlers['x'] = lambda e: (_ for _ in ()).throw(
            PermanentError('malformed')
        )

        worker.handle(Event(type='x', payload={}, attempt=1))

        assert len(store.dead_letters()) == 1
        assert broker.depth() == 0
        assert worker.stats.retried == 0

    def test_failed_event_releases_its_claim_so_the_retry_can_run(self, store, broker):
        # If the claim leaked, the retry would be dismissed as a duplicate and
        # the event would vanish.
        worker = self._worker(store, broker)
        event = Event(type='x', payload={})
        worker._handlers['x'] = lambda e: (_ for _ in ()).throw(RuntimeError('boom'))

        worker.handle(event)

        assert store.has_processed(event.id, worker.name) is False

    def test_retry_succeeds_on_the_second_attempt(self, store, broker):
        worker = self._worker(store, broker)
        calls = []

        def flaky(event):
            calls.append(event.attempt)
            if event.attempt == 1:
                raise RuntimeError('transient')

        worker._handlers['x'] = flaky
        worker.handle(Event(type='x', payload={}))
        worker.handle(broker.consume())

        assert calls == [1, 2]
        assert worker.stats.processed == 1
        assert store.dead_letters() == []

    def test_backoff_grows_exponentially_and_is_capped(self):
        policy = RetryPolicy(base_delay=1.0, max_delay=5.0)

        assert policy.delay_for(1) == 1.0
        assert policy.delay_for(2) == 2.0
        assert policy.delay_for(3) == 4.0
        assert policy.delay_for(10) == 5.0

    def test_unknown_event_type_is_acknowledged_not_requeued(self, store, broker):
        worker = self._worker(store, broker)

        assert worker.handle(Event(type='nobody.handles.this', payload={})) is True
        assert broker.depth() == 0
        assert worker.stats.unhandled == 1

    def test_dead_letter_can_be_replayed(self, store, broker):
        worker = self._worker(store, broker, max_attempts=1)
        event = Event(type='x', payload={'k': 'v'})
        worker._handlers['x'] = lambda e: (_ for _ in ()).throw(RuntimeError('boom'))

        worker.handle(event)
        assert len(store.dead_letters()) == 1

        replayed = store.replay_dead_letter(event.id)
        assert replayed is not None
        assert replayed.id == event.id
        assert replayed.attempt == 1  # attempt counter resets
        assert store.dead_letters() == []

    def test_replaying_an_unknown_id_returns_none(self, store):
        assert store.replay_dead_letter('nope') is None


class TestEndToEnd:
    def test_placed_order_reaches_reserved_status(self, pipeline):
        store, broker, relay, worker = pipeline
        store.place_order(make_order('ORD-9'), placed_event('ORD-9'))

        relay.drain()
        worker.drain()

        assert store.get_order('ORD-9').status == 'reserved'
        assert worker.stats.processed == 1

    def test_full_lifecycle_placed_to_confirmed(self, pipeline):
        store, broker, relay, worker = pipeline
        store.place_order(make_order('ORD-9'), placed_event('ORD-9'))
        relay.drain()
        worker.drain()

        store.enqueue_event(Event(type=ORDER_PAID, payload={'order_id': 'ORD-9'}))
        relay.drain()
        worker.drain()

        assert store.get_order('ORD-9').status == 'confirmed'

    def test_cancellation_is_applied(self, pipeline):
        store, broker, relay, worker = pipeline
        store.place_order(make_order('ORD-9'), placed_event('ORD-9'))
        relay.drain()
        worker.drain()

        store.enqueue_event(Event(type=ORDER_CANCELLED, payload={'order_id': 'ORD-9'}))
        relay.drain()
        worker.drain()

        assert store.get_order('ORD-9').status == 'cancelled'

    def test_event_for_a_missing_order_dead_letters(self, pipeline):
        store, broker, relay, worker = pipeline
        worker.retry = RetryPolicy(max_attempts=1)

        store.enqueue_event(placed_event('ORD-GHOST'))
        relay.drain()
        worker.drain()

        assert len(store.dead_letters()) == 1

    def test_event_missing_its_order_id_dead_letters_immediately(self, pipeline):
        store, broker, relay, worker = pipeline

        store.enqueue_event(Event(type=ORDER_PLACED, payload={}))
        relay.drain()
        worker.drain()

        letters = store.dead_letters()
        assert len(letters) == 1
        assert 'permanent' in letters[0]['error']
        assert worker.stats.retried == 0

    def test_many_orders_flow_through_without_loss(self, pipeline):
        store, broker, relay, worker = pipeline
        for i in range(40):
            store.place_order(make_order(f'ORD-{i:03d}'), placed_event(f'ORD-{i:03d}'))

        relay.drain()
        worker.drain()

        reserved = [o for o in store.list_orders(100) if o.status == 'reserved']
        assert len(reserved) == 40
        assert store.outbox_depth() == 0
        assert store.dead_letters() == []


class TestBrokerResolution:
    def test_memory_is_the_default(self):
        assert resolve_broker('memory').name == 'memory'

    def test_unknown_broker_is_rejected(self):
        with pytest.raises(BrokerError, match='Unknown broker'):
            resolve_broker('kafka-ish')

    def test_requeue_puts_it_back(self, broker):
        event = placed_event()
        broker.requeue(event)

        assert broker.depth() == 1
        assert broker.consume().id == event.id

    def test_consume_on_empty_returns_none(self, broker):
        assert broker.consume(timeout=0.01) is None


class TestApi:
    @pytest.fixture
    def client(self):
        services = Services(
            store=Store(), broker=InMemoryBroker(), run_background=False
        )
        with TestClient(create_app(services)) as client:
            client.services = services
            yield client
        services.store.close()

    def test_health_reports_pipeline_state(self, client):
        body = client.get('/health').json()

        assert body['status'] == 'ok'
        assert body['broker'] == 'memory'
        assert ORDER_PLACED in body['handlers']

    def test_placing_an_order_writes_order_and_outbox(self, client):
        response = client.post(
            '/orders', json={'customer': 'Acme', 'amount': 250.0}
        )

        assert response.status_code == 201
        assert response.json()['status'] == 'placed'

        health = client.get('/health').json()
        assert health['orders'] == 1
        assert health['outbox_pending'] == 1

    def test_order_amount_must_be_positive(self, client):
        response = client.post('/orders', json={'customer': 'Acme', 'amount': -5})
        assert response.status_code == 422

    def test_customer_is_required(self, client):
        response = client.post('/orders', json={'customer': '', 'amount': 10})
        assert response.status_code == 422

    def test_order_is_retrievable(self, client):
        order_id = client.post(
            '/orders', json={'customer': 'Acme', 'amount': 10}
        ).json()['id']

        assert client.get(f'/orders/{order_id}').json()['id'] == order_id

    def test_unknown_order_is_404(self, client):
        assert client.get('/orders/ORD-NOPE').status_code == 404

    def test_pay_transitions_the_order_after_processing(self, client):
        order_id = client.post(
            '/orders', json={'customer': 'Acme', 'amount': 10}
        ).json()['id']

        client.services.relay.drain()
        client.services.worker.drain()

        assert client.post(f'/orders/{order_id}/pay').status_code == 202
        client.services.relay.drain()
        client.services.worker.drain()

        assert client.get(f'/orders/{order_id}').json()['status'] == 'confirmed'

    def test_paying_an_unknown_order_is_404(self, client):
        assert client.post('/orders/ORD-NOPE/pay').status_code == 404

    def test_cancel_transitions_the_order(self, client):
        order_id = client.post(
            '/orders', json={'customer': 'Acme', 'amount': 10}
        ).json()['id']

        client.services.relay.drain()
        client.services.worker.drain()
        client.post(f'/orders/{order_id}/cancel')
        client.services.relay.drain()
        client.services.worker.drain()

        assert client.get(f'/orders/{order_id}').json()['status'] == 'cancelled'

    def test_dead_letters_are_listed_and_replayable(self, client):
        services = client.services
        services.worker.retry = RetryPolicy(max_attempts=1)

        # An event for an order that does not exist fails and dead-letters.
        services.store.enqueue_event(placed_event('ORD-GHOST'))
        services.relay.drain()
        services.worker.drain()

        letters = client.get('/dead-letters').json()
        assert len(letters) == 1

        event_id = letters[0]['event_id']
        assert client.post(f'/dead-letters/{event_id}/replay').status_code == 202
        assert client.get('/dead-letters').json() == []

    def test_replaying_an_unknown_dead_letter_is_404(self, client):
        assert client.post('/dead-letters/nope/replay').status_code == 404
