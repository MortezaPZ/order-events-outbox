"""Business handlers registered on a worker.

Each one is a plain function that raises to signal failure. Retry, idempotency
and dead-lettering are the worker's concern, so handlers stay readable.
"""

from __future__ import annotations

import logging

from .events import ORDER_CANCELLED, ORDER_PAID, ORDER_PLACED, Event
from .store import Store
from .worker import PermanentError, Worker

logger = logging.getLogger(__name__)


def register(worker: Worker, store: Store) -> Worker:
    @worker.on(ORDER_PLACED)
    def reserve_stock(event: Event) -> None:
        order_id = event.payload.get('order_id')
        if not order_id:
            # No amount of retrying adds a missing field.
            raise PermanentError('order.placed is missing order_id.')

        if not store.update_order_status(order_id, 'reserved'):
            raise RuntimeError(f'Order {order_id} not found; may not be committed yet.')

        logger.info('Reserved stock for %s', order_id)

    @worker.on(ORDER_PAID)
    def confirm_order(event: Event) -> None:
        order_id = event.payload.get('order_id')
        if not order_id:
            raise PermanentError('order.paid is missing order_id.')

        if not store.update_order_status(order_id, 'confirmed'):
            raise RuntimeError(f'Order {order_id} not found.')

        logger.info('Confirmed %s', order_id)

    @worker.on(ORDER_CANCELLED)
    def release_stock(event: Event) -> None:
        order_id = event.payload.get('order_id')
        if not order_id:
            raise PermanentError('order.cancelled is missing order_id.')

        store.update_order_status(order_id, 'cancelled')
        logger.info('Cancelled %s', order_id)

    return worker
