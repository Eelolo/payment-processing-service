import asyncio
import logging

from faststream.rabbit import RabbitBroker
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import OutboxEvent
from app.messaging.events import ATTEMPT_HEADER
from app.messaging.topology import PAYMENTS_EXCHANGE

logger = logging.getLogger(__name__)

MAX_ERROR_LENGTH = 1000


class OutboxRelay:
    """Publishes outbox events to RabbitMQ, oldest first.

    Delivery is at least once: an event is marked published only after the broker
    confirms it, so a crash between the confirm and the commit re-publishes it (the
    consumer is idempotent, and message_id = event id allows deduplication).
    Rows are claimed with FOR UPDATE SKIP LOCKED, so several relay replicas can run
    side by side without publishing the same event twice. Ordering is best effort (strict
    with one replica); consumers do not rely on it, every event is processed on its own.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        *,
        batch_size: int,
        poll_interval_s: float,
        max_backoff_s: float,
        publish_timeout_s: float,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._batch_size = batch_size
        self._poll_interval_s = poll_interval_s
        self._max_backoff_s = max_backoff_s
        self._publish_timeout_s = publish_timeout_s

    async def run(self) -> None:
        delay = self._poll_interval_s
        while True:
            try:
                published, failed = await self.publish_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                failed, published = True, 0
            # Broker or database trouble: back off exponentially instead of hammering it.
            delay = min(delay * 2, self._max_backoff_s) if failed else self._poll_interval_s
            # A full batch means there is a backlog: continue right away.
            if failed or published < self._batch_size:
                await asyncio.sleep(delay)

    async def publish_batch(self) -> tuple[int, bool]:
        """Publish one batch. Returns (published count, whether a publish failed)."""
        async with self._session_factory() as session, session.begin():
            events = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.created_at)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            published = 0
            for event in events:
                try:
                    await self._publish(event)
                except Exception as exc:
                    # A failure almost always means the broker is unavailable: stop the batch
                    # and back off instead of failing every remaining event the same way.
                    event.attempts += 1
                    event.last_error = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH]
                    logger.warning(
                        "Outbox publish failed (attempt %d): event_id=%s error=%s",
                        event.attempts,
                        event.id,
                        event.last_error,
                    )
                    return published, True
                event.published_at = func.now()
                published += 1
        if published:
            logger.info("Outbox: published %d event(s)", published)
        return published, False

    async def _publish(self, event: OutboxEvent) -> None:
        await self._broker.publish(
            event.payload,
            exchange=PAYMENTS_EXCHANGE.name,
            routing_key=event.routing_key,
            message_id=str(event.id),
            message_type=event.event_type,
            headers={ATTEMPT_HEADER: 1},
            persist=True,
            mandatory=True,
            timeout=self._publish_timeout_s,
        )
