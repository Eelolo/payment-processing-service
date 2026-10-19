import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import Currency, Payment
from app.schemas import PaymentCreate
from app.services.payments import create_payment

WEBHOOK_URL = "http://client.test/webhooks"


def payment_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": WEBHOOK_URL,
    }
    return body | overrides


async def make_payment(session_factory: async_sessionmaker[AsyncSession]) -> uuid.UUID:
    data = PaymentCreate(
        amount="10.00", currency=Currency.USD, description="test", webhook_url=WEBHOOK_URL
    )
    async with session_factory() as session:
        result = await create_payment(session, data, idempotency_key=str(uuid.uuid4()))
    return result.payment.id


async def load_payment(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID
) -> Payment:
    async with session_factory() as session:
        return (await session.scalars(select(Payment).where(Payment.id == payment_id))).one()
