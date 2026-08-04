"""SQLite storage: orders, the outbox, and consumer bookkeeping.

The outbox is the point of this module. An order and the event announcing it
are written in **one transaction**, so it is impossible to end up with an order
that was never announced, or an announcement for an order that was rolled back.
Publishing happens afterwards, from the outbox, by a separate relay.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from .events import Event

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id          TEXT PRIMARY KEY,
    customer    TEXT NOT NULL,
    amount      REAL NOT NULL,
    currency    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'placed',
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id            TEXT PRIMARY KEY,
    event_type    TEXT NOT NULL,
    body          TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    published_at  TEXT
);

-- The relay only ever scans unpublished rows, so index exactly that.
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox(created_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    consumer     TEXT NOT NULL,
    processed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letters (
    event_id    TEXT PRIMARY KEY,
    event_type  TEXT NOT NULL,
    body        TEXT NOT NULL,
    error       TEXT NOT NULL,
    attempts    INTEGER NOT NULL,
    failed_at   TEXT NOT NULL
);
"""


class StoreError(RuntimeError):
    """Raised when a store operation cannot complete."""


@dataclass(frozen=True)
class Order:
    id: str
    customer: str
    amount: float
    currency: str
    status: str
    created_at: str


def _now() -> str:
    return datetime.now(UTC).isoformat()


class Store:
    def __init__(self, path: str | Path = ':memory:') -> None:
        self.path = str(path)
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute('PRAGMA foreign_keys = ON')
        # WAL lets the relay read while the API writes.
        if self.path != ':memory:':
            self._connection.execute('PRAGMA journal_mode = WAL')
        self._connection.executescript(SCHEMA)
        self._lock = threading.Lock()

    def close(self) -> None:
        self._connection.close()

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def place_order(self, order: Order, event: Event) -> None:
        """Persist the order and its event atomically.

        If this raises, neither row exists. If it returns, both do. There is no
        window in which the order is visible but unannounced.
        """
        with self._lock, self._connection:
            existing = self._connection.execute(
                'SELECT 1 FROM orders WHERE id = ?', (order.id,)
            ).fetchone()
            if existing:
                raise StoreError(f'Order "{order.id}" already exists.')

            self._connection.execute(
                'INSERT INTO orders (id, customer, amount, currency, status, created_at) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (
                    order.id,
                    order.customer,
                    order.amount,
                    order.currency,
                    order.status,
                    order.created_at,
                ),
            )
            self._connection.execute(
                'INSERT INTO outbox (id, event_type, body, created_at) '
                'VALUES (?, ?, ?, ?)',
                (event.id, event.type, event.to_json(), _now()),
            )

    def update_order_status(self, order_id: str, status: str) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                'UPDATE orders SET status = ? WHERE id = ?', (status, order_id)
            )
        return cursor.rowcount > 0

    # ------------------------------------------------------------------
    # Outbox
    # ------------------------------------------------------------------

    def enqueue_event(self, event: Event) -> None:
        """Add an event to the outbox on its own.

        Used for state transitions that do not create a row of their own. Going
        through the outbox rather than publishing directly keeps every event on
        one delivery path, so ordering and the crash guarantee still hold.
        """
        with self._lock, self._connection:
            self._connection.execute(
                'INSERT INTO outbox (id, event_type, body, created_at) '
                'VALUES (?, ?, ?, ?)',
                (event.id, event.type, event.to_json(), _now()),
            )

    def pending_outbox(self, limit: int = 100) -> list[Event]:
        rows = self._connection.execute(
            'SELECT body FROM outbox WHERE published_at IS NULL '
            'ORDER BY created_at LIMIT ?',
            (limit,),
        ).fetchall()
        return [Event.from_json(row['body']) for row in rows]

    def mark_published(self, event_ids: list[str]) -> int:
        if not event_ids:
            return 0
        placeholders = ','.join('?' * len(event_ids))
        with self._lock, self._connection:
            cursor = self._connection.execute(
                f'UPDATE outbox SET published_at = ? '
                f'WHERE id IN ({placeholders}) AND published_at IS NULL',
                (_now(), *event_ids),
            )
        return cursor.rowcount

    def outbox_depth(self) -> int:
        return int(
            self._connection.execute(
                'SELECT COUNT(*) FROM outbox WHERE published_at IS NULL'
            ).fetchone()[0]
        )

    # ------------------------------------------------------------------
    # Consumer bookkeeping
    # ------------------------------------------------------------------

    def claim_event(self, event_id: str, consumer: str) -> bool:
        """Record that `consumer` is handling `event_id`.

        Returns False if this consumer already handled it — the check and the
        insert are one atomic statement, so two workers racing on a redelivered
        message cannot both win.
        """
        try:
            with self._lock, self._connection:
                self._connection.execute(
                    'INSERT INTO processed_events (event_id, consumer, processed_at) '
                    'VALUES (?, ?, ?)',
                    (f'{consumer}:{event_id}', consumer, _now()),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def has_processed(self, event_id: str, consumer: str) -> bool:
        row = self._connection.execute(
            'SELECT 1 FROM processed_events WHERE event_id = ?',
            (f'{consumer}:{event_id}',),
        ).fetchone()
        return row is not None

    def release_claim(self, event_id: str, consumer: str) -> None:
        """Undo a claim so a transient failure can be retried."""
        with self._lock, self._connection:
            self._connection.execute(
                'DELETE FROM processed_events WHERE event_id = ?',
                (f'{consumer}:{event_id}',),
            )

    # ------------------------------------------------------------------
    # Dead letters
    # ------------------------------------------------------------------

    def dead_letter(self, event: Event, error: str) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                'INSERT OR REPLACE INTO dead_letters '
                '(event_id, event_type, body, error, attempts, failed_at) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (event.id, event.type, event.to_json(), error, event.attempt, _now()),
            )

    def dead_letters(self) -> list[dict]:
        rows = self._connection.execute(
            'SELECT event_id, event_type, error, attempts, failed_at '
            'FROM dead_letters ORDER BY failed_at DESC'
        ).fetchall()
        return [dict(row) for row in rows]

    def replay_dead_letter(self, event_id: str) -> Event | None:
        """Pull a poison message back out for reprocessing after a fix."""
        row = self._connection.execute(
            'SELECT body FROM dead_letters WHERE event_id = ?', (event_id,)
        ).fetchone()
        if row is None:
            return None

        with self._lock, self._connection:
            self._connection.execute(
                'DELETE FROM dead_letters WHERE event_id = ?', (event_id,)
            )
        return Event.from_json(row['body']).with_attempt(1)

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_order(self, order_id: str) -> Order | None:
        row = self._connection.execute(
            'SELECT * FROM orders WHERE id = ?', (order_id,)
        ).fetchone()
        return Order(**dict(row)) if row else None

    def list_orders(self, limit: int = 50) -> list[Order]:
        rows = self._connection.execute(
            'SELECT * FROM orders ORDER BY created_at DESC LIMIT ?', (limit,)
        ).fetchall()
        return [Order(**dict(row)) for row in rows]

    def count_orders(self) -> int:
        return int(
            self._connection.execute('SELECT COUNT(*) FROM orders').fetchone()[0]
        )
