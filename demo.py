"""Walk through the three guarantees this service exists to provide."""

from datetime import UTC, datetime

from orderflow.broker import FlakyBroker, InMemoryBroker
from orderflow.events import ORDER_PAID, ORDER_PLACED, Event
from orderflow.handlers import register
from orderflow.relay import OutboxRelay
from orderflow.store import Order, Store
from orderflow.worker import RetryPolicy, Worker


def order(order_id: str, amount: float) -> Order:
    return Order(
        id=order_id,
        customer='Acme Ltd',
        amount=amount,
        currency='GBP',
        status='placed',
        created_at=datetime.now(UTC).isoformat(),
    )


def placed(order_id: str) -> Event:
    return Event(type=ORDER_PLACED, payload={'order_id': order_id})


def rule(title: str) -> None:
    print(f'\n{"=" * 68}\n{title}\n{"=" * 68}')


def happy_path() -> None:
    rule('1. Normal flow — order placed, event delivered, status advanced')

    store, broker = Store(), InMemoryBroker()
    relay = OutboxRelay(store, broker)
    worker = register(Worker(store, broker, sleep=lambda _: None), store)

    store.place_order(order('ORD-001', 249.99), placed('ORD-001'))
    print(f'  placed          order=1  outbox_pending={store.outbox_depth()}')

    relay.drain()
    print(
        f'  relay published queue={broker.depth()}  '
        f'outbox_pending={store.outbox_depth()}'
    )

    worker.drain()
    print(f'  worker done     status={store.get_order("ORD-001").status}')

    store.enqueue_event(Event(type=ORDER_PAID, payload={'order_id': 'ORD-001'}))
    relay.drain()
    worker.drain()
    print(f'  after payment   status={store.get_order("ORD-001").status}')
    store.close()


def broker_outage() -> None:
    rule('2. Broker outage — the outbox holds the event until delivery works')

    store = Store()
    flaky = FlakyBroker(fail_times=2)
    relay = OutboxRelay(store, flaky)
    worker = register(Worker(store, flaky, sleep=lambda _: None), store)

    store.place_order(order('ORD-002', 89.50), placed('ORD-002'))

    for attempt in (1, 2, 3):
        delivered = relay.drain_once()
        state = 'delivered' if delivered else 'BROKER DOWN'
        print(f'  pass {attempt}: {state:<12} outbox_pending={store.outbox_depth()}')

    worker.drain()
    print(f'  nothing lost    status={store.get_order("ORD-002").status}')
    store.close()


def duplicate_delivery() -> None:
    rule('3. Duplicate delivery — at-least-once, but the effect runs once')

    store, broker = Store(), InMemoryBroker()
    relay = OutboxRelay(store, broker)
    worker = register(Worker(store, broker, sleep=lambda _: None), store)

    store.place_order(order('ORD-003', 15.00), placed('ORD-003'))
    relay.drain()
    event = broker.consume()

    for delivery in (1, 2, 3):
        worker.handle(event)
        print(
            f'  delivery {delivery}: processed={worker.stats.processed} '
            f'duplicates_skipped={worker.stats.duplicates}'
        )
    store.close()


def poison_message() -> None:
    rule('4. Poison message — bounded retries, then dead-letter, then replay')

    store, broker = Store(), InMemoryBroker()
    worker = register(
        Worker(store, broker, retry=RetryPolicy(max_attempts=3), sleep=lambda _: None),
        store,
    )

    # No such order, so the handler keeps failing.
    ghost = placed('ORD-GHOST')
    broker.publish(ghost)

    attempt = 0
    while (event := broker.consume(timeout=0.01)) is not None:
        attempt += 1
        worker.handle(event)
        print(f'  attempt {attempt}: retried={worker.stats.retried} '
              f'dead_lettered={worker.stats.dead_lettered}')

    letters = store.dead_letters()
    print(f'  dead letters:   {len(letters)}')
    print(f'  reason:         {letters[0]["error"][:60]}…')

    # The order shows up (bug fixed), so the replay now succeeds.
    store.place_order(order('ORD-GHOST', 5.00), placed('ORD-GHOST-2'))
    replayed = store.replay_dead_letter(ghost.id)
    broker.publish(replayed)
    worker.drain()

    print(f'  after replay:   status={store.get_order("ORD-GHOST").status}  '
          f'dead_letters={len(store.dead_letters())}')
    store.close()


def throughput() -> None:
    rule('5. Bulk flow — 500 orders, nothing lost')

    store, broker = Store(), InMemoryBroker()
    relay = OutboxRelay(store, broker)
    worker = register(Worker(store, broker, sleep=lambda _: None), store)

    for i in range(500):
        store.place_order(order(f'ORD-{i:04d}', 10.0 + i), placed(f'ORD-{i:04d}'))

    published = relay.drain()
    worker.drain(max_events=2000)

    reserved = sum(1 for o in store.list_orders(1000) if o.status == 'reserved')
    print(f'  orders placed:  {store.count_orders()}')
    print(f'  events relayed: {published}')
    print(f'  reserved:       {reserved}')
    print(f'  outbox left:    {store.outbox_depth()}')
    print(f'  dead letters:   {len(store.dead_letters())}')
    store.close()


if __name__ == '__main__':
    happy_path()
    broker_outage()
    duplicate_delivery()
    poison_message()
    throughput()
    print()
