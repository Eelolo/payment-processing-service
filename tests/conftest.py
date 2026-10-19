"""Tests run against real PostgreSQL and RabbitMQ (see docker-compose.test.yml).

ON CONFLICT, FOR UPDATE SKIP LOCKED, TTL and dead-lettering are exactly the behaviour
under test, so they are not mocked.
"""

import asyncio
import os
from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest
from alembic import command
from alembic.config import Config
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.api.main import create_app
from app.config import Settings, get_settings
from app.db.session import create_engine, create_session_factory
from app.messaging.topology import PAYMENTS_DLQ, PAYMENTS_NEW_QUEUE, retry_queue

os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("WEBHOOK_SECRET", "test-webhook-secret")
os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://payments:payments@localhost:5432/payments_test"
)
# Short delays keep the retry tests fast; the logic does not depend on the values.
os.environ.setdefault("RETRY_BASE_DELAY_MS", "100")

API_KEY = os.environ["API_KEY"]


@pytest.fixture(scope="session")
def settings() -> Settings:
    get_settings.cache_clear()
    return get_settings()


async def _ensure_database(database_url: str) -> None:
    url = make_url(database_url)
    conn = await asyncpg.connect(
        user=url.username, password=url.password, host=url.host, port=url.port, database="postgres"
    )
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", url.database)
        if not exists:
            await conn.execute(f'CREATE DATABASE "{url.database}"')
    finally:
        await conn.close()


@pytest.fixture(scope="session")
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    await _ensure_database(settings.database_url)
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", settings.database_url)
    # The migration env runs its own event loop, so it cannot share the test loop.
    await asyncio.to_thread(command.downgrade, alembic_cfg, "base")
    await asyncio.to_thread(command.upgrade, alembic_cfg, "head")
    engine = create_engine(settings)
    yield engine
    await engine.dispose()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))
    yield create_session_factory(engine)


@pytest.fixture
async def client(
    engine: AsyncEngine, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app()
    # ASGITransport does not run the lifespan, so wire the state the lifespan would set.
    app.state.engine = engine
    app.state.session_factory = session_factory
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", headers={"X-API-Key": API_KEY}
    ) as client:
        yield client


@pytest.fixture
async def broker(settings: Settings) -> AsyncIterator[RabbitBroker]:
    broker = RabbitBroker(settings.rabbitmq_url, logger=None)
    await broker.connect()
    for queue in (PAYMENTS_NEW_QUEUE, PAYMENTS_DLQ, *map(retry_queue, settings.retry_delays_ms)):
        declared = await broker.declare_queue(queue)
        await declared.purge()
    yield broker
    await broker.stop()
