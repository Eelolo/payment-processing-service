import asyncio
import uuid

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxEvent, Payment
from app.messaging.events import PAYMENT_CREATED
from tests.factories import payment_body

URL = "/api/v1/payments"


async def counts(session_factory: async_sessionmaker[AsyncSession]) -> tuple[int, int]:
    async with session_factory() as session:
        payments = await session.scalar(select(func.count()).select_from(Payment))
        events = await session.scalar(select(func.count()).select_from(OutboxEvent))
    return payments or 0, events or 0


async def post(client: httpx.AsyncClient, key: str, **overrides: object) -> httpx.Response:
    return await client.post(URL, json=payment_body(**overrides), headers={"Idempotency-Key": key})


async def test_create_payment_writes_payment_and_outbox_event(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await post(client, "key-1")

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert set(body) == {"payment_id", "status", "created_at"}
    async with session_factory() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.event_type == PAYMENT_CREATED
    assert event.payload == {"payment_id": body["payment_id"]}
    assert event.published_at is None


async def test_get_payment_returns_details(client: httpx.AsyncClient) -> None:
    payment_id = (await post(client, "key-1")).json()["payment_id"]

    response = await client.get(f"{URL}/{payment_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == payment_id
    assert body["amount"] == "1500.50"  # decimal as string, never a float
    assert body["currency"] == "RUB"
    assert body["metadata"] == {"order_id": 42}
    assert body["idempotency_key"] == "key-1"
    assert body["processed_at"] is None


async def test_replay_with_same_key_returns_same_payment_without_new_event(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    first = await post(client, "key-1")
    replay = await post(client, "key-1")

    assert replay.status_code == 202
    assert replay.json() == first.json()
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert "Idempotent-Replayed" not in first.headers
    assert await counts(session_factory) == (1, 1)


async def test_equivalent_amount_notation_is_a_replay(client: httpx.AsyncClient) -> None:
    first = await post(client, "key-1", amount=100)
    replay = await post(client, "key-1", amount="100.00")

    assert replay.status_code == 202
    assert replay.json()["payment_id"] == first.json()["payment_id"]


async def test_same_key_with_different_body_is_rejected(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await post(client, "key-1")

    response = await post(client, "key-1", amount="1.00")

    assert response.status_code == 409
    assert await counts(session_factory) == (1, 1)


async def test_concurrent_requests_with_same_key_create_one_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    responses = await asyncio.gather(*(post(client, "same-key") for _ in range(10)))

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await counts(session_factory) == (1, 1)


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": "0"},
        {"amount": "-5"},
        {"amount": "1.001"},
        {"currency": "GBP"},
        {"webhook_url": "not-a-url"},
        {"description": ""},
        {"unexpected": "field"},
        {"metadata": {"blob": "x" * 20_000}},
    ],
)
async def test_invalid_body_is_rejected(
    client: httpx.AsyncClient, overrides: dict[str, str]
) -> None:
    response = await post(client, str(uuid.uuid4()), **overrides)

    assert response.status_code == 422


@pytest.mark.parametrize("headers", [{}, {"Idempotency-Key": "   "}], ids=["missing", "blank"])
async def test_idempotency_key_is_required(
    client: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    response = await client.post(URL, json=payment_body(), headers=headers)

    assert response.status_code == 422


async def test_unknown_payment_is_404(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{URL}/{uuid.uuid4()}")

    assert response.status_code == 404


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
async def test_every_endpoint_requires_api_key(
    client: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    client.headers.pop("X-API-Key")
    client.headers.update(headers)

    create = await client.post(URL, json=payment_body(), headers={"Idempotency-Key": "k"})
    get = await client.get(f"{URL}/{uuid.uuid4()}")

    assert create.status_code == 401
    assert get.status_code == 401
