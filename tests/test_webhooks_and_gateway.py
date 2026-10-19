import asyncio
import json
import random
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.consumer.gateway import EmulatedGateway
from app.consumer.webhooks import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    WebhookDeliveryError,
    WebhookNotifier,
    sign,
)
from app.db.models import Currency, Payment, PaymentStatus

SECRET = b"secret"


def finished_payment() -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("99.90"),
        currency=Currency.EUR,
        description="test",
        metadata_={"order_id": 7},
        status=PaymentStatus.SUCCEEDED,
        webhook_url="http://client.test/hook",
        processed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


async def test_webhook_is_signed_and_carries_the_result() -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200)

    payment = finished_payment()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await WebhookNotifier(client, SECRET).send(payment)

    (request,) = captured
    body = json.loads(request.content)
    timestamp = request.headers[TIMESTAMP_HEADER]
    assert request.headers[SIGNATURE_HEADER] == sign(request.content, SECRET, timestamp)
    assert request.headers[SIGNATURE_HEADER] != sign(request.content, SECRET, "0")
    assert body["event"] == "payment.succeeded"
    assert body["payment_id"] == str(payment.id)
    assert body["amount"] == "99.90"
    assert body["metadata"] == {"order_id": 7}


def time_out(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectTimeout("timed out", request=request)


@pytest.mark.parametrize(
    "respond",
    [lambda _: httpx.Response(500), lambda _: httpx.Response(404), time_out],
    ids=["5xx", "4xx", "timeout"],
)
async def test_failed_delivery_raises(respond: Callable[[httpx.Request], httpx.Response]) -> None:
    transport = httpx.MockTransport(respond)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(WebhookDeliveryError):
            await WebhookNotifier(client, SECRET).send(finished_payment())


async def test_gateway_emulation_respects_delay_bounds_and_success_rate() -> None:
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    gateway = EmulatedGateway(
        min_delay_s=2, max_delay_s=5, success_rate=0.9, rng=random.Random(42), sleep=fake_sleep
    )
    results = [await gateway.charge(uuid.uuid4(), Decimal(1), Currency.RUB) for _ in range(2000)]

    assert all(2 <= d <= 5 for d in delays)
    success_share = sum(r.succeeded for r in results) / len(results)
    assert 0.87 < success_share < 0.93


async def test_gateway_is_idempotent_per_payment() -> None:
    """Duplicate and concurrent charges of one payment return the first result."""
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await asyncio.sleep(0)

    gateway = EmulatedGateway(min_delay_s=2, max_delay_s=5, success_rate=0.5, sleep=fake_sleep)
    payment_id = uuid.uuid4()

    concurrent = await asyncio.gather(
        *(gateway.charge(payment_id, Decimal(1), Currency.RUB) for _ in range(5))
    )
    later = await gateway.charge(payment_id, Decimal(1), Currency.RUB)

    assert len(sleeps) == 1  # charged once
    assert len({r.succeeded for r in [*concurrent, later]}) == 1
