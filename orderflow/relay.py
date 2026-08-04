"""Outbox relay: moves committed events from the database onto the broker.

This is the half of the outbox pattern that people skip. Writing the event to
the outbox guarantees it was *recorded*; the relay is what guarantees it gets
*delivered*. Because it only marks a row published after the broker accepted it,
a broker outage delays delivery rather than losing the event.

Delivery is therefore at-least-once — a crash between publish and mark means a
redelivery. That is the consumer's problem to absorb, which is why consumers
are idempotent.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from .broker import Broker, BrokerError
from .store import Store

logger = logging.getLogger(__name__)


@dataclass
class RelayStats:
    published: int = 0
    failed: int = 0
    batches: int = 0


class OutboxRelay:
    def __init__(
        self,
        store: Store,
        broker: Broker,
        batch_size: int = 100,
        interval: float = 0.5,
    ) -> None:
        self.store = store
        self.broker = broker
        self.batch_size = batch_size
        self.interval = interval
        self.stats = RelayStats()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def drain_once(self) -> int:
        """Publish one batch. Returns how many events were delivered."""
        pending = self.store.pending_outbox(self.batch_size)
        if not pending:
            return 0

        self.stats.batches += 1
        delivered: list[str] = []

        for event in pending:
            try:
                self.broker.publish(event)
            except BrokerError as exc:
                # Stop at the first failure so ordering is preserved; the row
                # stays unpublished and the next pass retries it.
                self.stats.failed += 1
                logger.warning('Relay could not publish %s: %s', event.id, exc)
                break
            delivered.append(event.id)

        if delivered:
            self.store.mark_published(delivered)
            self.stats.published += len(delivered)

        return len(delivered)

    def drain(self, max_passes: int = 100) -> int:
        """Publish until the outbox is empty or nothing more can be delivered."""
        total = 0
        for _ in range(max_passes):
            delivered = self.drain_once()
            if delivered == 0:
                break
            total += delivered
        return total

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError('Relay is already running.')
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name='relay')
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.drain_once()
            except Exception:
                logger.exception('Relay pass failed')
            self._stop.wait(self.interval)
