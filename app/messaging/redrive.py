"""Move messages from the DLQ back to payments.new once the cause is fixed.

    docker compose exec consumer python -m app.messaging.redrive [--limit N]

Safe to run at any time: the consumer is idempotent, so a payment that was already
finalized only gets its pending webhook re-sent. Each message starts a fresh attempt count.
"""

import argparse
import asyncio
import logging

from faststream.rabbit import Channel, RabbitBroker

from app.config import get_settings
from app.log import setup_logging
from app.messaging.events import ATTEMPT_HEADER, LAST_ERROR_HEADER
from app.messaging.topology import (
    PAYMENTS_DLQ,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_ROUTING_KEY,
    declare_topology,
)

logger = logging.getLogger(__name__)

# Headers that describe the previous life of the message and must not follow it back.
DROPPED_HEADERS = {
    LAST_ERROR_HEADER,
    "x-death",
    "x-first-death-exchange",
    "x-first-death-queue",
    "x-first-death-reason",
    "x-last-death-exchange",
    "x-last-death-queue",
    "x-last-death-reason",
}


async def redrive(limit: int | None) -> int:
    settings = get_settings()
    broker = RabbitBroker(
        settings.rabbitmq_url, default_channel=Channel(on_return_raises=True), logger=None
    )
    await broker.connect()
    try:
        await declare_topology(broker, settings.retry_delays_ms)
        dlq = await broker.declare_queue(PAYMENTS_DLQ)
        moved = 0
        while limit is None or moved < limit:
            message = await dlq.get(fail=False)
            if message is None:
                break
            headers = {k: v for k, v in message.headers.items() if k not in DROPPED_HEADERS}
            await broker.publish(
                message.body,
                exchange=PAYMENTS_EXCHANGE.name,
                routing_key=PAYMENTS_NEW_ROUTING_KEY,
                headers={**headers, ATTEMPT_HEADER: 1},
                message_id=message.message_id,
                content_type=message.content_type,
                persist=True,
                mandatory=True,
                timeout=settings.broker_publish_timeout_s,
            )
            # Ack only after the confirmed publish: a crash in between duplicates, never loses.
            await message.ack()
            moved += 1
        return moved
    finally:
        await broker.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--limit", type=int, default=None, help="move at most N messages")
    args = parser.parse_args()
    setup_logging(get_settings().log_level)
    moved = asyncio.run(redrive(args.limit))
    logger.info(
        "Moved %d message(s) from %s to %s", moved, PAYMENTS_DLQ.name, PAYMENTS_EXCHANGE.name
    )


if __name__ == "__main__":
    main()
