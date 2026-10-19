import asyncio
import logging
import random
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from app.db.models import Currency

logger = logging.getLogger(__name__)

# How many charge results the emulator remembers for idempotent replays. Redeliveries
# arrive within seconds, so this only has to outlive the retry window.
IDEMPOTENCY_CACHE_SIZE = 10_000


@dataclass(frozen=True, slots=True)
class ChargeResult:
    succeeded: bool


class PaymentGateway(Protocol):
    async def charge(
        self, payment_id: uuid.UUID, amount: Decimal, currency: Currency
    ) -> ChargeResult:
        """Charge the payment. A decline is a normal result, not an exception.

        payment_id doubles as the gateway-side idempotency key, as real gateways expect.
        """
        ...


class EmulatedGateway:
    """External gateway emulation: random latency and a fixed share of declines.

    Idempotent on payment_id like a real gateway: a repeated or concurrent charge of the
    same payment (a duplicate message, a retry after the status update failed) gets the
    original result instead of a second charge. The memory is per process, which stands in
    for the gateway's own storage.
    """

    def __init__(
        self,
        *,
        min_delay_s: float,
        max_delay_s: float,
        success_rate: float,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._min_delay_s = min_delay_s
        self._max_delay_s = max_delay_s
        self._success_rate = success_rate
        self._rng = rng or random.Random()
        self._sleep = sleep
        self._charges: OrderedDict[uuid.UUID, asyncio.Task[ChargeResult]] = OrderedDict()

    async def charge(
        self, payment_id: uuid.UUID, amount: Decimal, currency: Currency
    ) -> ChargeResult:
        task = self._charges.get(payment_id)
        if task is None:
            task = asyncio.create_task(self._charge(payment_id, amount, currency))
            self._charges[payment_id] = task
            while len(self._charges) > IDEMPOTENCY_CACHE_SIZE:
                self._charges.popitem(last=False)
        else:
            logger.info("Gateway idempotent replay: payment_id=%s", payment_id)
        # shield: a cancelled caller must not cancel the charge other callers wait for.
        return await asyncio.shield(task)

    async def _charge(
        self, payment_id: uuid.UUID, amount: Decimal, currency: Currency
    ) -> ChargeResult:
        await self._sleep(self._rng.uniform(self._min_delay_s, self._max_delay_s))
        succeeded = self._rng.random() < self._success_rate
        logger.info(
            "Gateway %s: payment_id=%s amount=%s %s",
            "approved" if succeeded else "declined",
            payment_id,
            amount,
            currency,
        )
        return ChargeResult(succeeded=succeeded)
