import logging
import uuid

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import PaymentGateway
from app.consumer.webhooks import WebhookNotifier
from app.db.models import Payment, PaymentStatus

logger = logging.getLogger(__name__)


class PaymentNotFoundError(Exception):
    """The event references a payment that does not exist. Retrying will not help."""


class PaymentProcessor:
    """Brings a payment to its final state and notifies the client. Safe to run repeatedly.

    Messages are delivered at least once (outbox relay and broker retries can both
    duplicate them), so every step checks what is already done:
    - the gateway is called only while the payment is pending, and the status is written
      with compare-and-set, so a duplicate can neither charge twice nor flip the result;
    - the webhook is skipped once delivered, so a retry caused by a failed webhook
      re-sends only the webhook.
    No transaction is held open during the slow gateway and webhook calls.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        notifier: WebhookNotifier,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: uuid.UUID) -> None:
        payment = await self._load(payment_id)
        if payment.status is PaymentStatus.PENDING:
            payment = await self._charge(payment)
        else:
            logger.info(
                "Payment already %s, skipping gateway: payment_id=%s", payment.status, payment_id
            )

        if payment.webhook_delivered_at is not None:
            logger.info("Webhook already delivered: payment_id=%s", payment_id)
            return
        await self._notifier.send(payment)
        await self._mark_webhook_delivered(payment_id)
        logger.info("Webhook delivered: payment_id=%s status=%s", payment_id, payment.status)

    async def _load(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFoundError(f"Payment {payment_id} not found")
        return payment

    async def _charge(self, payment: Payment) -> Payment:
        result = await self._gateway.charge(payment.id, payment.amount, payment.currency)
        new_status = PaymentStatus.SUCCEEDED if result.succeeded else PaymentStatus.FAILED
        async with self._session_factory() as session, session.begin():
            finalized = await session.scalar(
                update(Payment)
                .where(Payment.id == payment.id, Payment.status == PaymentStatus.PENDING)
                .values(status=new_status, processed_at=func.now())
                .returning(Payment)
            )
            if finalized is None:
                # A concurrent duplicate finished first; its result is the one that counts.
                finalized = await session.scalar(select(Payment).where(Payment.id == payment.id))
                logger.warning(
                    "Payment finalized concurrently, keeping status %s: payment_id=%s",
                    finalized.status if finalized else None,
                    payment.id,
                )
        if finalized is None:
            raise PaymentNotFoundError(f"Payment {payment.id} not found")
        logger.info("Payment %s: payment_id=%s", finalized.status, payment.id)
        return finalized

    async def _mark_webhook_delivered(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
                .values(webhook_delivered_at=func.now())
            )
