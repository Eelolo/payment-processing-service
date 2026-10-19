"""Consumer process: `faststream run app.consumer.main:app`."""

import httpx
from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker

from app.config import Settings, get_settings
from app.consumer.gateway import EmulatedGateway
from app.consumer.handler import register_payment_handler
from app.consumer.processor import PaymentProcessor
from app.consumer.retry import RetryScheduler
from app.consumer.webhooks import WebhookNotifier
from app.db.session import create_engine, create_session_factory
from app.log import setup_logging
from app.messaging.topology import declare_topology


def create_app(settings: Settings) -> FastStream:
    setup_logging(settings.log_level)
    broker = RabbitBroker(
        settings.rabbitmq_url,
        # Messages are processed concurrently up to prefetch: each one mostly waits on I/O.
        default_channel=Channel(prefetch_count=settings.consumer_prefetch, on_return_raises=True),
        graceful_timeout=settings.consumer_graceful_timeout_s,
    )
    engine = create_engine(settings)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout_s)
    processor = PaymentProcessor(
        session_factory=create_session_factory(engine),
        gateway=EmulatedGateway(
            min_delay_s=settings.gateway_min_delay_s,
            max_delay_s=settings.gateway_max_delay_s,
            success_rate=settings.gateway_success_rate,
        ),
        notifier=WebhookNotifier(http_client, settings.webhook_secret.get_secret_value().encode()),
    )
    retries = RetryScheduler(broker, settings.retry_delays_ms, settings.broker_publish_timeout_s)
    register_payment_handler(broker, processor, retries)

    async def on_startup() -> None:
        # Declare retry and DLQ queues before the subscriber starts consuming.
        await broker.connect()
        await declare_topology(broker, settings.retry_delays_ms)

    async def after_shutdown() -> None:
        await http_client.aclose()
        await engine.dispose()

    return FastStream(broker, on_startup=[on_startup], after_shutdown=[after_shutdown])


app = create_app(get_settings())
