import hashlib
import json
import logging
import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent, Payment
from app.messaging.events import PAYMENT_AGGREGATE, PAYMENT_CREATED, PaymentCreatedEvent
from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY
from app.schemas import PaymentCreate

logger = logging.getLogger(__name__)


class IdempotencyKeyReusedError(Exception):
    """The idempotency key was already used with a different request body."""


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool  # False when an earlier request with the same key is replayed


def request_fingerprint(data: PaymentCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> CreatePaymentResult:
    """Create a payment and its outbox event in one transaction, or replay an earlier request.

    Duplicate detection relies on the unique index, not on "SELECT then INSERT": with
    ON CONFLICT DO NOTHING a concurrent request with the same key waits for the first
    transaction and then inserts nothing, so exactly one payment and one event exist.
    """
    fingerprint = request_fingerprint(data)
    insert_stmt = (
        insert(Payment)
        .values(
            id=uuid.uuid4(),
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            idempotency_key=idempotency_key,
            request_hash=fingerprint,
            webhook_url=str(data.webhook_url),
        )
        .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
        .returning(Payment)
    )
    payment = (await session.scalars(insert_stmt)).one_or_none()

    if payment is None:
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None:
            # Unreachable while payments are never deleted: the conflict means the row exists.
            raise RuntimeError(f"Payment with idempotency key {idempotency_key!r} vanished")
        if existing.request_hash != fingerprint:
            raise IdempotencyKeyReusedError
        logger.info("Idempotent replay: key=%s payment_id=%s", idempotency_key, existing.id)
        return CreatePaymentResult(payment=existing, created=False)

    session.add(
        OutboxEvent(
            aggregate_type=PAYMENT_AGGREGATE,
            aggregate_id=payment.id,
            event_type=PAYMENT_CREATED,
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            payload=PaymentCreatedEvent(payment_id=payment.id).model_dump(mode="json"),
        )
    )
    await session.commit()
    logger.info(
        "Payment created: payment_id=%s amount=%s %s", payment.id, payment.amount, payment.currency
    )
    return CreatePaymentResult(payment=payment, created=True)


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
