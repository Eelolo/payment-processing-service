import asyncio

from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config import Settings


async def test_migrations_match_models(engine: AsyncEngine, settings: Settings) -> None:
    """Autogenerate finds nothing to add: models and migrations describe the same schema."""
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", settings.database_url)

    await asyncio.to_thread(command.check, alembic_cfg)
