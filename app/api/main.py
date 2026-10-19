import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api.routes import router
from app.config import get_settings
from app.db.session import create_engine, create_session_factory
from app.log import setup_logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    setup_logging(settings.log_level)
    engine = create_engine(settings)
    app.state.engine = engine
    app.state.session_factory = create_session_factory(engine)
    yield
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="Payment Processing Service", version="0.1.0", lifespan=lifespan)
    app.include_router(router)

    @app.get("/health", tags=["service"], include_in_schema=False)
    async def health(request: Request) -> JSONResponse:
        """Readiness: the API is useless without the database, so check it."""
        try:
            async with request.app.state.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except Exception as exc:
            logger.warning("Health check failed: %r", exc)
            return JSONResponse(
                {"status": "unavailable", "detail": type(exc).__name__},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        return JSONResponse({"status": "ok"})

    return app


app = create_app()
