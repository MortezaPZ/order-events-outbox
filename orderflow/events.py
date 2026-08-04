"""Event envelope shared by producers and consumers.

Every message carries its own id, type and version. The id is what makes
consumers idempotent; the version is what lets a consumer reject a payload
shape it does not understand instead of misreading it.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = 1


class EventError(ValueError):
    """Raised when an event cannot be built or decoded."""


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class Event:
    type: str
    payload: dict[str, Any]
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: int = SCHEMA_VERSION
    occurred_at: str = field(default_factory=lambda: utcnow().isoformat())
    # Set by the broker when a message is redelivered after a failure.
    attempt: int = 1

    def __post_init__(self) -> None:
        if not self.type:
            raise EventError('Event type is required.')
        if not isinstance(self.payload, dict):
            raise EventError('Event payload must be an object.')

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(',', ':'), sort_keys=True)

    @staticmethod
    def from_json(raw: str) -> Event:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise EventError(f'Event is not valid JSON: {exc}') from exc

        if not isinstance(data, dict):
            raise EventError('Event must decode to an object.')

        missing = [key for key in ('id', 'type', 'payload') if key not in data]
        if missing:
            raise EventError(f'Event is missing fields: {missing}')

        version = int(data.get('version', SCHEMA_VERSION))
        if version > SCHEMA_VERSION:
            # Refusing a newer payload is safer than silently misreading it
            # during a rolling deploy.
            raise EventError(
                f'Event schema v{version} is newer than this consumer '
                f'understands (v{SCHEMA_VERSION}).'
            )

        return Event(
            id=str(data['id']),
            type=str(data['type']),
            payload=dict(data['payload']),
            version=version,
            occurred_at=str(data.get('occurred_at', utcnow().isoformat())),
            attempt=int(data.get('attempt', 1)),
        )

    def with_attempt(self, attempt: int) -> Event:
        return Event(
            id=self.id,
            type=self.type,
            payload=self.payload,
            version=self.version,
            occurred_at=self.occurred_at,
            attempt=attempt,
        )


# Event type constants — string literals scattered through the codebase are how
# producer and consumer drift apart.
ORDER_PLACED = 'order.placed'
ORDER_PAID = 'order.paid'
ORDER_CANCELLED = 'order.cancelled'
