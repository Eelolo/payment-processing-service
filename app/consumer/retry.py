import logging
import uuid
from typing import Any

from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from app.messaging.events import ATTEMPT_HEADER, LAST_ERROR_HEADER, MAX_ERROR_HEADER_LENGTH
from app.messaging.topology import DLQ_ROUTING_KEY, DLX_EXCHANGE, RETRY_EXCHANGE, retry_queue_name

logger = logging.getLogger(__name__)


def attempt_of(message: RabbitMessage) -> int:
    """Delivery attempt of the message, 1 for the first one."""
    return int(message.headers.get(ATTEMPT_HEADER, 1))


def _describe(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}"[:MAX_ERROR_HEADER_LENGTH]


class RetryScheduler:
    """Routes a failed message to the next delay queue, or to the DLQ when attempts run out.

    The caller acks the original only after this returns: the copy is published with
    publisher confirms first, so a crash in between gives a duplicate, never a lost message.
    """

    def __init__(
        self, broker: RabbitBroker, retry_delays_ms: list[int], publish_timeout_s: float
    ) -> None:
        self._broker = broker
        self._publish_timeout_s = publish_timeout_s
        self._retry_delays_ms = retry_delays_ms
        self._max_attempts = len(retry_delays_ms) + 1

    async def retry_or_dead_letter(
        self, message: RabbitMessage, error: BaseException, payment_id: uuid.UUID
    ) -> None:
        attempt = attempt_of(message)
        if attempt >= self._max_attempts:
            await self.dead_letter(message, error, payment_id)
            return
        delay_ms = self._retry_delays_ms[attempt - 1]
        await self._republish(
            message,
            exchange=RETRY_EXCHANGE.name,
            routing_key=retry_queue_name(delay_ms),
            headers={ATTEMPT_HEADER: attempt + 1, LAST_ERROR_HEADER: _describe(error)},
        )
        logger.warning(
            "Attempt %d/%d failed, retry in %d ms: payment_id=%s message_id=%s error=%s",
            attempt,
            self._max_attempts,
            delay_ms,
            payment_id,
            message.message_id,
            _describe(error),
        )

    async def dead_letter(
        self, message: RabbitMessage, error: BaseException, payment_id: uuid.UUID
    ) -> None:
        attempt = attempt_of(message)
        await self._republish(
            message,
            exchange=DLX_EXCHANGE.name,
            routing_key=DLQ_ROUTING_KEY,
            headers={ATTEMPT_HEADER: attempt, LAST_ERROR_HEADER: _describe(error)},
        )
        logger.error(
            "Moved to DLQ after %d attempt(s): payment_id=%s message_id=%s error=%s",
            attempt,
            payment_id,
            message.message_id,
            _describe(error),
        )

    async def _republish(
        self, message: RabbitMessage, *, exchange: str, routing_key: str, headers: dict[str, Any]
    ) -> None:
        await self._broker.publish(
            message.body,
            exchange=exchange,
            routing_key=routing_key,
            headers={**message.headers, **headers},
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            content_type=message.content_type,
            persist=True,
            # With on_return_raises on the channel, an unroutable copy fails here instead of
            # being confirmed, acked and silently lost.
            mandatory=True,
            timeout=self._publish_timeout_s,
        )
