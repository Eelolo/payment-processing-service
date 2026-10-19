"""Outbox relay and retry/DLQ routing against a real RabbitMQ."""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any, cast

import aio_pika
import pytest
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.consumer.handler import register_payment_handler
from app.consumer.processor import PaymentNotFoundError
from app.consumer.retry import RetryScheduler
from app.consumer.webhooks import WebhookDeliveryError
from app.db.models import OutboxEvent
from app.messaging.events import ATTEMPT_HEADER, LAST_ERROR_HEADER
from app.messaging.redrive import redrive
from app.messaging.topology import (
    DLX_EXCHANGE,
    PAYMENTS_DLQ,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    declare_topology,
)
from app.relay.outbox import OutboxRelay
from tests.factories import make_payment

WAIT_TIMEOUT_S = 10


async def wait_for_message(broker: RabbitBroker, queue: RabbitQueue) -> aio_pika.IncomingMessage:
    declared = await broker.declare_queue(queue)
    async with asyncio.timeout(WAIT_TIMEOUT_S):
        # Polling is the only option: basic.get has no "wait for a message" mode.
        while (message := await declared.get(fail=False, no_ack=True)) is None:  # noqa: ASYNC110
            await asyncio.sleep(0.05)
    return message


def make_relay(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> OutboxRelay:
    return OutboxRelay(
        session_factory,
        broker,
        batch_size=10,
        poll_interval_s=0.1,
        max_backoff_s=1,
        publish_timeout_s=5,
    )


async def test_relay_publishes_pending_events_and_marks_them(
    session_factory: async_sessionmaker[AsyncSession],
    broker: RabbitBroker,
    settings: Settings,
) -> None:
    await declare_topology(broker, settings.retry_delays_ms)
    payment_id = await make_payment(session_factory)

    published, failed = await make_relay(session_factory, broker).publish_batch()

    assert (published, failed) == (1, False)
    message = await wait_for_message(broker, PAYMENTS_NEW_QUEUE)
    assert json.loads(message.body) == {"payment_id": str(payment_id)}
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    async with session_factory() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.published_at is not None
    assert message.message_id == str(event.id)
    # Nothing left: a second run publishes nothing.
    assert await make_relay(session_factory, broker).publish_batch() == (0, False)


async def test_relay_keeps_events_and_order_when_broker_fails(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first = await make_payment(session_factory)
    await make_payment(session_factory)

    class BrokenBroker:
        async def publish(self, *args: Any, **kwargs: Any) -> None:
            raise ConnectionError("broker is down")

    relay = make_relay(session_factory, BrokenBroker())  # type: ignore[arg-type]

    assert await relay.publish_batch() == (0, True)
    async with session_factory() as session:
        events = (await session.scalars(select(OutboxEvent).order_by(OutboxEvent.created_at))).all()
    assert all(e.published_at is None for e in events)
    # Stopped at the first failure: only the head event was attempted.
    assert [e.attempts for e in events] == [1, 0]
    assert events[0].aggregate_id == first
    assert "broker is down" in (events[0].last_error or "")


class FlakyProcessor:
    def __init__(self, error: Callable[[], Exception], failures: int) -> None:
        self.error = error
        self.failures = failures
        self.calls = 0
        self.done = asyncio.Event()

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error()
        self.done.set()


@pytest.fixture
async def consumer(
    broker: RabbitBroker, settings: Settings
) -> AsyncIterator[Callable[[FlakyProcessor], Any]]:
    """Run the real payments.new handler with a stub processor."""
    consumer_broker = RabbitBroker(
        settings.rabbitmq_url, default_channel=Channel(on_return_raises=True), logger=None
    )

    async def start(processor: FlakyProcessor) -> None:
        register_payment_handler(
            consumer_broker,
            processor,
            RetryScheduler(consumer_broker, settings.retry_delays_ms, publish_timeout_s=5),
        )
        await consumer_broker.connect()
        await declare_topology(consumer_broker, settings.retry_delays_ms)
        await consumer_broker.start()

    yield start
    await consumer_broker.stop()


async def publish_event(broker: RabbitBroker) -> None:
    await broker.publish(
        {"payment_id": str(uuid.uuid4())},
        exchange=PAYMENTS_EXCHANGE,
        routing_key=PAYMENTS_NEW_QUEUE.routing(),
        headers={ATTEMPT_HEADER: 1},
    )


async def test_transient_failure_is_retried_until_success(
    broker: RabbitBroker, consumer: Callable[[FlakyProcessor], Any], settings: Settings
) -> None:
    processor = FlakyProcessor(
        lambda: WebhookDeliveryError("500"), failures=settings.max_attempts - 1
    )
    await consumer(processor)

    await publish_event(broker)

    async with asyncio.timeout(WAIT_TIMEOUT_S):
        await processor.done.wait()
    assert processor.calls == settings.max_attempts
    dlq = await broker.declare_queue(PAYMENTS_DLQ)
    assert await dlq.get(fail=False) is None


async def test_message_goes_to_dlq_after_max_attempts(
    broker: RabbitBroker, consumer: Callable[[FlakyProcessor], Any], settings: Settings
) -> None:
    processor = FlakyProcessor(lambda: WebhookDeliveryError("500"), failures=100)
    await consumer(processor)

    await publish_event(broker)

    message = await wait_for_message(broker, PAYMENTS_DLQ)
    assert processor.calls == settings.max_attempts
    assert message.headers[ATTEMPT_HEADER] == settings.max_attempts
    assert "WebhookDeliveryError" in str(message.headers[LAST_ERROR_HEADER])


async def test_permanent_failure_goes_to_dlq_without_retries(
    broker: RabbitBroker, consumer: Callable[[FlakyProcessor], Any]
) -> None:
    processor = FlakyProcessor(lambda: PaymentNotFoundError("gone"), failures=100)
    await consumer(processor)

    await publish_event(broker)

    message = await wait_for_message(broker, PAYMENTS_DLQ)
    assert processor.calls == 1
    assert message.headers[ATTEMPT_HEADER] == 1


async def test_undecodable_message_is_dead_lettered_by_the_broker(
    broker: RabbitBroker, consumer: Callable[[FlakyProcessor], Any]
) -> None:
    processor = FlakyProcessor(RuntimeError, failures=0)
    await consumer(processor)

    await broker.publish(
        b"not json", exchange=PAYMENTS_EXCHANGE, routing_key=PAYMENTS_NEW_QUEUE.routing()
    )

    message = await wait_for_message(broker, PAYMENTS_DLQ)
    assert message.body == b"not json"
    (death,) = cast(list[dict[str, Any]], message.headers["x-death"])
    assert death["reason"] == "rejected"
    assert processor.calls == 0


async def test_redrive_returns_dead_letters_with_a_fresh_attempt_count(
    broker: RabbitBroker, settings: Settings
) -> None:
    await declare_topology(broker, settings.retry_delays_ms)
    await broker.publish(
        {"payment_id": str(uuid.uuid4())},
        exchange=DLX_EXCHANGE,
        routing_key=PAYMENTS_DLQ.routing(),
        headers={ATTEMPT_HEADER: settings.max_attempts, LAST_ERROR_HEADER: "boom"},
        message_id="event-1",
    )
    # No waiting needed: the publish returned after the broker confirmed the message is queued.

    assert await redrive(limit=None) == 1

    message = await wait_for_message(broker, PAYMENTS_NEW_QUEUE)
    assert message.message_id == "event-1"
    assert message.headers[ATTEMPT_HEADER] == 1
    assert LAST_ERROR_HEADER not in message.headers
    dlq = await broker.declare_queue(PAYMENTS_DLQ)
    assert await dlq.get(fail=False) is None
