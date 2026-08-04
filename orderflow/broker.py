"""Message broker abstraction.

`InMemoryBroker` is the default: it makes the whole system runnable and testable
with no infrastructure. `RedisBroker` is the same interface over a real queue.
Nothing above this module knows which one it is talking to.
"""

from __future__ import annotations

import os
import queue
import threading
from typing import Protocol, runtime_checkable

from .events import Event


class BrokerError(RuntimeError):
    """Raised when a broker operation fails."""


@runtime_checkable
class Broker(Protocol):
    name: str

    def publish(self, event: Event) -> None:
        ...

    def consume(self, timeout: float = 0.1) -> Event | None:
        """Return the next event, or None if the queue is empty."""

    def requeue(self, event: Event) -> None:
        """Put an event back for another attempt."""

    def depth(self) -> int:
        ...


class InMemoryBroker:
    """Thread-safe in-process queue.

    Good enough for a single-process deployment and for tests. It deliberately
    does not persist: durability is the outbox's job, not the queue's, which is
    exactly why the outbox pattern exists.
    """

    name = 'memory'

    def __init__(self) -> None:
        self._queue: queue.Queue[Event] = queue.Queue()
        self._published = 0
        self._lock = threading.Lock()

    def publish(self, event: Event) -> None:
        with self._lock:
            self._published += 1
        self._queue.put(event)

    def consume(self, timeout: float = 0.1) -> Event | None:
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def requeue(self, event: Event) -> None:
        self._queue.put(event)

    def depth(self) -> int:
        return self._queue.qsize()

    @property
    def published_count(self) -> int:
        with self._lock:
            return self._published


class FlakyBroker(InMemoryBroker):
    """In-memory broker that fails the first N publishes.

    Used to prove the outbox actually protects against a broker outage: the
    events stay pending and are published once the broker recovers.
    """

    name = 'flaky'

    def __init__(self, fail_times: int = 1) -> None:
        super().__init__()
        self.fail_times = fail_times
        self.attempts = 0

    def publish(self, event: Event) -> None:
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise BrokerError(f'Broker unavailable (attempt {self.attempts}).')
        super().publish(event)


class RedisBroker:
    """Redis list used as a FIFO queue."""

    name = 'redis'

    def __init__(self, url: str | None = None, key: str = 'orderflow:events') -> None:
        try:
            import redis
        except ImportError as exc:  # pragma: no cover - depends on environment
            raise BrokerError(
                'The redis package is not installed. '
                'Install it, or use BROKER=memory.'
            ) from exc

        self.key = key
        self._client = redis.Redis.from_url(
            url or os.environ.get('REDIS_URL', 'redis://localhost:6379/0'),
            decode_responses=True,
        )

    def publish(self, event: Event) -> None:
        self._client.rpush(self.key, event.to_json())

    def consume(self, timeout: float = 0.1) -> Event | None:
        # blpop takes whole seconds; round up so a sub-second timeout still waits.
        result = self._client.blpop([self.key], timeout=max(1, int(timeout + 0.999)))
        return Event.from_json(result[1]) if result else None

    def requeue(self, event: Event) -> None:
        self._client.rpush(self.key, event.to_json())

    def depth(self) -> int:
        return int(self._client.llen(self.key))


def resolve_broker(name: str | None = None) -> Broker:
    choice = (name or os.environ.get('BROKER') or 'memory').lower()
    if choice == 'memory':
        return InMemoryBroker()
    if choice == 'redis':
        return RedisBroker()
    raise BrokerError(f'Unknown broker "{choice}".')
