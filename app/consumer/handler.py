import logging
import uuid
from typing import Protocol

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from app.consumer.processor import PaymentNotFoundError
from app.consumer.retry import RetryScheduler
from app.consumer.webhooks import WebhookDeliveryError
from app.messaging.events import PaymentCreatedEvent
from app.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE

logger = logging.getLogger(__name__)


class Processor(Protocol):
    async def process(self, payment_id: uuid.UUID) -> None: ...


def register_payment_handler(
    broker: RabbitBroker, processor: Processor, retries: RetryScheduler
) -> None:
    """Subscribe the single payments.new consumer.

    REJECT_ON_ERROR covers messages that cannot even be decoded: they are rejected without
    requeue and dead-lettered by the queue's DLX. Processing errors are caught here and go
    through the delay queues; if scheduling the retry itself fails, the exception reaches
    the middleware and the message is dead-lettered rather than lost.
    """

    @broker.subscriber(PAYMENTS_NEW_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def handle_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        try:
            await processor.process(event.payment_id)
        except PaymentNotFoundError as exc:
            await retries.dead_letter(message, exc, event.payment_id)
        except WebhookDeliveryError as exc:
            # Expected failure mode: logged by the scheduler, no traceback needed.
            await retries.retry_or_dead_letter(message, exc, event.payment_id)
        except Exception as exc:
            logger.exception("Unexpected processing error: payment_id=%s", event.payment_id)
            await retries.retry_or_dead_letter(message, exc, event.payment_id)
