import uuid
from decimal import Decimal

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import ChargeResult
from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.consumer.webhooks import WebhookDeliveryError
from app.db.models import Currency, Payment, PaymentStatus
from tests.factories import load_payment, make_payment


class FakeGateway:
    def __init__(self, *, succeeded: bool = True) -> None:
        self.succeeded = succeeded
        self.calls: list[uuid.UUID] = []

    async def charge(
        self, payment_id: uuid.UUID, amount: Decimal, currency: Currency
    ) -> ChargeResult:
        self.calls.append(payment_id)
        return ChargeResult(succeeded=self.succeeded)


class FakeNotifier:
    def __init__(self, *, failures: int = 0) -> None:
        self.failures = failures
        self.sent: list[tuple[uuid.UUID, PaymentStatus]] = []

    async def send(self, payment: Payment) -> None:
        if self.failures:
            self.failures -= 1
            raise WebhookDeliveryError("503 from client")
        self.sent.append((payment.id, payment.status))


def make_processor(
    session_factory: async_sessionmaker[AsyncSession],
    gateway: FakeGateway,
    notifier: FakeNotifier,
) -> PaymentProcessor:
    return PaymentProcessor(session_factory, gateway, notifier)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("succeeded", "expected"), [(True, PaymentStatus.SUCCEEDED), (False, PaymentStatus.FAILED)]
)
async def test_process_finalizes_payment_and_notifies(
    session_factory: async_sessionmaker[AsyncSession],
    succeeded: bool,
    expected: PaymentStatus,
) -> None:
    payment_id = await make_payment(session_factory)
    notifier = FakeNotifier()

    await make_processor(session_factory, FakeGateway(succeeded=succeeded), notifier).process(
        payment_id
    )

    payment = await load_payment(session_factory, payment_id)
    assert payment.status is expected
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    assert notifier.sent == [(payment_id, expected)]


async def test_redelivered_message_neither_charges_nor_notifies_again(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await make_payment(session_factory)
    gateway, notifier = FakeGateway(), FakeNotifier()
    processor = make_processor(session_factory, gateway, notifier)

    await processor.process(payment_id)
    await processor.process(payment_id)

    assert gateway.calls == [payment_id]
    assert len(notifier.sent) == 1


async def test_webhook_retry_does_not_charge_again(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await make_payment(session_factory)
    gateway, notifier = FakeGateway(), FakeNotifier(failures=1)
    processor = make_processor(session_factory, gateway, notifier)

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment_id)
    after_failure = await load_payment(session_factory, payment_id)
    await processor.process(payment_id)

    assert after_failure.status is PaymentStatus.SUCCEEDED
    assert after_failure.webhook_delivered_at is None
    assert gateway.calls == [payment_id]
    assert notifier.sent == [(payment_id, PaymentStatus.SUCCEEDED)]


async def test_result_of_concurrent_duplicate_is_kept(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Another worker finalizes the payment while this one waits for the gateway."""
    payment_id = await make_payment(session_factory)

    class RacingGateway(FakeGateway):
        async def charge(
            self, payment_id: uuid.UUID, amount: Decimal, currency: Currency
        ) -> ChargeResult:
            async with session_factory() as session, session.begin():
                await session.execute(
                    update(Payment)
                    .where(Payment.id == payment_id)
                    .values(status=PaymentStatus.FAILED)
                )
            return ChargeResult(succeeded=True)

    notifier = FakeNotifier()

    await make_processor(session_factory, RacingGateway(), notifier).process(payment_id)

    payment = await load_payment(session_factory, payment_id)
    assert payment.status is PaymentStatus.FAILED
    assert notifier.sent == [(payment_id, PaymentStatus.FAILED)]


async def test_unknown_payment_raises_not_found(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    processor = make_processor(session_factory, FakeGateway(), FakeNotifier())

    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid.uuid4())
