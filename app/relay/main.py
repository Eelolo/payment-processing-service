"""Outbox relay process: `faststream run app.relay.main:app`."""

import asyncio
import contextlib

from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker

from app.config import Settings, get_settings
from app.db.session import create_engine, create_session_factory
from app.log import setup_logging
from app.messaging.topology import declare_topology
from app.relay.outbox import OutboxRelay


def create_app(settings: Settings) -> FastStream:
    setup_logging(settings.log_level)
    # on_return_raises: an unroutable message must fail the publish, otherwise the relay
    # would mark it published while the broker silently dropped it.
    broker = RabbitBroker(settings.rabbitmq_url, default_channel=Channel(on_return_raises=True))
    engine = create_engine(settings)
    relay = OutboxRelay(
        create_session_factory(engine),
        broker,
        batch_size=settings.outbox_batch_size,
        poll_interval_s=settings.outbox_poll_interval_s,
        max_backoff_s=settings.outbox_max_backoff_s,
        publish_timeout_s=settings.broker_publish_timeout_s,
    )
    tasks: set[asyncio.Task[None]] = set()

    async def after_startup() -> None:
        await declare_topology(broker, settings.retry_delays_ms)
        tasks.add(asyncio.create_task(relay.run(), name="outbox-relay"))

    async def on_shutdown() -> None:
        for task in tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await engine.dispose()

    return FastStream(broker, after_startup=[after_startup], on_shutdown=[on_shutdown])


app = create_app(get_settings())
