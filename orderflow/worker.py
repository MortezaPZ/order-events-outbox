"""Consumer: idempotent handling, bounded retries, dead-letter on give-up.

Delivery is at-least-once, so the same event can arrive twice. Every handler
runs behind a claim: the first worker to claim an event id processes it, and a
duplicate is acknowledged without re-running the side effect.

A handler that raises is retried with exponential backoff. Once attempts are
exhausted the event goes to the dead-letter table rather than back on the queue,
because a poison message that requeues forever blocks everything behind it.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from .broker import Broker
from .events import Event
from .store import Store

logger = logging.getLogger(__name__)

Handler = Callable[[Event], None]


class PermanentError(Exception):
    """Raised by a handler for an event that will never succeed.

    Skips the retry ladder and dead-letters immediately — retrying a malformed
    payload five times just wastes time and log space.
    """


@dataclass
class WorkerStats:
    processed: int = 0
    duplicates: int = 0
    retried: int = 0
    dead_lettered: int = 0
    unhandled: int = 0


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 0.05
    max_delay: float = 5.0

    def delay_for(self, attempt: int) -> float:
        """Exponential backoff, capped."""
        return min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)


class Worker:
    def __init__(
        self,
        store: Store,
        broker: Broker,
        name: str = 'worker',
        retry: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.store = store
        self.broker = broker
        self.name = name
        self.retry = retry or RetryPolicy()
        self.stats = WorkerStats()
        self._handlers: dict[str, Handler] = {}
        self._sleep = sleep
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def on(self, event_type: str) -> Callable[[Handler], Handler]:
        def decorator(handler: Handler) -> Handler:
            self._handlers[event_type] = handler
            return handler

        return decorator

    @property
    def handled_types(self) -> list[str]:
        return sorted(self._handlers)

    def handle(self, event: Event) -> bool:
        """Process one event. Returns True if it was handled or safely skipped."""
        handler = self._handlers.get(event.type)
        if handler is None:
            # Unknown type: acknowledge rather than requeue forever. Another
            # service in the fleet may be the intended consumer.
            self.stats.unhandled += 1
            logger.debug('No handler for %s; skipping', event.type)
            return True

        # Claim before doing the work, so a concurrent redelivery cannot run
        # the same side effect twice.
        if not self.store.claim_event(event.id, self.name):
            self.stats.duplicates += 1
            logger.debug('Event %s already handled by %s', event.id, self.name)
            return True

        try:
            handler(event)
        except PermanentError as exc:
            self.store.release_claim(event.id, self.name)
            self.store.dead_letter(event, f'permanent: {exc}')
            self.stats.dead_lettered += 1
            logger.warning('Event %s dead-lettered permanently: %s', event.id, exc)
            return False
        except Exception as exc:
            # The claim must be released or the retry would be treated as a
            # duplicate and silently dropped.
            self.store.release_claim(event.id, self.name)

            if event.attempt >= self.retry.max_attempts:
                self.store.dead_letter(
                    event, f'failed after {event.attempt} attempts: {exc}'
                )
                self.stats.dead_lettered += 1
                logger.warning(
                    'Event %s dead-lettered after %s attempts', event.id, event.attempt
                )
                return False

            self._sleep(self.retry.delay_for(event.attempt))
            self.broker.requeue(event.with_attempt(event.attempt + 1))
            self.stats.retried += 1
            logger.info(
                'Event %s requeued (attempt %s): %s', event.id, event.attempt + 1, exc
            )
            return False

        self.stats.processed += 1
        return True

    def drain(self, max_events: int = 1000, timeout: float = 0.05) -> int:
        """Consume until the queue is empty. Returns how many were pulled."""
        pulled = 0
        for _ in range(max_events):
            event = self.broker.consume(timeout=timeout)
            if event is None:
                break
            self.handle(event)
            pulled += 1
        return pulled

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError('Worker is already running.')
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name=f'worker-{self.name}'
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                event = self.broker.consume(timeout=0.1)
                if event is not None:
                    self.handle(event)
            except Exception:
                logger.exception('Worker loop failed')
