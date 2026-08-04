"""HTTP surface: place orders, inspect state, operate the dead-letter queue."""

from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from .broker import Broker, resolve_broker
from .events import ORDER_CANCELLED, ORDER_PAID, ORDER_PLACED, Event
from .handlers import register
from .relay import OutboxRelay
from .store import Order, Store, StoreError
from .worker import Worker


class PlaceOrderRequest(BaseModel):
    customer: str = Field(min_length=1, max_length=200)
    amount: float = Field(gt=0, le=1_000_000)
    currency: str = Field(default='GBP', min_length=3, max_length=3)


class OrderResponse(BaseModel):
    id: str
    customer: str
    amount: float
    currency: str
    status: str
    created_at: str


class TransitionRequest(BaseModel):
    order_id: str = Field(min_length=1)


class DeadLetterResponse(BaseModel):
    event_id: str
    event_type: str
    error: str
    attempts: int
    failed_at: str


class Services:
    """Wires the pieces together and owns their lifetime."""

    def __init__(
        self,
        store: Store | None = None,
        broker: Broker | None = None,
        run_background: bool = True,
    ) -> None:
        self.store = store or Store(os.environ.get('DB_PATH', 'orderflow.db'))
        self.broker = broker or resolve_broker()
        self.relay = OutboxRelay(self.store, self.broker)
        self.worker = register(Worker(self.store, self.broker), self.store)
        self.run_background = run_background

    def start(self) -> None:
        if self.run_background:
            self.relay.start()
            self.worker.start()

    def stop(self) -> None:
        if self.run_background:
            self.worker.stop()
            self.relay.stop()

    def publish(self, event_type: str, order_id: str) -> Event:
        """Queue a state-transition event through the outbox."""
        event = Event(type=event_type, payload={'order_id': order_id})
        self.store.enqueue_event(event)
        return event


def create_app(services: Services | None = None) -> FastAPI:
    resolved = services

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.services = resolved or Services()
        app.state.services.start()
        yield
        app.state.services.stop()

    app = FastAPI(
        title='Order Events',
        description=(
            'Event-driven order service using the transactional outbox pattern, '
            'idempotent consumers, bounded retries and a dead-letter queue.'
        ),
        version='1.0.0',
        lifespan=lifespan,
    )

    def get_services() -> Services:
        return app.state.services

    @app.get('/health')
    def health(services: Services = Depends(get_services)) -> dict:
        return {
            'status': 'ok',
            'broker': services.broker.name,
            'orders': services.store.count_orders(),
            'outbox_pending': services.store.outbox_depth(),
            'queue_depth': services.broker.depth(),
            'dead_letters': len(services.store.dead_letters()),
            'handlers': services.worker.handled_types,
        }

    @app.post('/orders', response_model=OrderResponse, status_code=201)
    def place_order(
        request: PlaceOrderRequest, services: Services = Depends(get_services)
    ):
        order = Order(
            id=f'ORD-{uuid.uuid4().hex[:12].upper()}',
            customer=request.customer,
            amount=request.amount,
            currency=request.currency.upper(),
            status='placed',
            created_at=datetime.now(UTC).isoformat(),
        )
        event = Event(
            type=ORDER_PLACED,
            payload={
                'order_id': order.id,
                'customer': order.customer,
                'amount': order.amount,
                'currency': order.currency,
            },
        )

        try:
            # Order and event commit together, or neither does.
            services.store.place_order(order, event)
        except StoreError as exc:
            raise HTTPException(409, str(exc)) from exc

        return OrderResponse(**order.__dict__)

    @app.get('/orders', response_model=list[OrderResponse])
    def list_orders(services: Services = Depends(get_services)):
        return [OrderResponse(**o.__dict__) for o in services.store.list_orders()]

    @app.get('/orders/{order_id}', response_model=OrderResponse)
    def get_order(order_id: str, services: Services = Depends(get_services)):
        order = services.store.get_order(order_id)
        if order is None:
            raise HTTPException(404, f'No order "{order_id}".')
        return OrderResponse(**order.__dict__)

    @app.post('/orders/{order_id}/pay', status_code=202)
    def pay_order(order_id: str, services: Services = Depends(get_services)):
        if services.store.get_order(order_id) is None:
            raise HTTPException(404, f'No order "{order_id}".')
        event = services.publish(ORDER_PAID, order_id)
        return {'event_id': event.id, 'type': event.type}

    @app.post('/orders/{order_id}/cancel', status_code=202)
    def cancel_order(order_id: str, services: Services = Depends(get_services)):
        if services.store.get_order(order_id) is None:
            raise HTTPException(404, f'No order "{order_id}".')
        event = services.publish(ORDER_CANCELLED, order_id)
        return {'event_id': event.id, 'type': event.type}

    @app.get('/dead-letters', response_model=list[DeadLetterResponse])
    def dead_letters(services: Services = Depends(get_services)):
        return [DeadLetterResponse(**d) for d in services.store.dead_letters()]

    @app.post('/dead-letters/{event_id}/replay', status_code=202)
    def replay(event_id: str, services: Services = Depends(get_services)):
        """Put a poison message back on the queue after the bug is fixed."""
        event = services.store.replay_dead_letter(event_id)
        if event is None:
            raise HTTPException(404, f'No dead letter "{event_id}".')

        # The failed attempt left a claim behind only on success paths; clear
        # any lingering one so the replay is not mistaken for a duplicate.
        services.store.release_claim(event.id, services.worker.name)
        services.broker.publish(event)
        return {'event_id': event.id, 'requeued': True}

    return app


app = create_app()
