"""Demo webhook receiver: records notifications and can simulate a failing client.

Not part of the service, it exists to make retries and the DLQ observable:
    POST /webhooks                    -> 200
    POST /webhooks?fail=always        -> 500 every time (message ends up in the DLQ)
    POST /webhooks?fail_first=2       -> 500 for the first 2 attempts of a payment, then 200
    GET  /webhooks/{payment_id}       -> every attempt received for the payment
"""

import hmac
import os
from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request, status

from app.consumer.webhooks import SIGNATURE_HEADER, TIMESTAMP_HEADER, sign

# The sink keeps history in memory, so it must stay bounded.
MAX_TRACKED_PAYMENTS = 1000

app = FastAPI(title="Webhook sink (demo)")
_secret = os.environ.get("WEBHOOK_SECRET", "").encode()
_attempts: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()


def _record(payment_id: str, entry: dict[str, Any]) -> int:
    attempts = _attempts.setdefault(payment_id, [])
    _attempts.move_to_end(payment_id)
    attempts.append(entry)
    while len(_attempts) > MAX_TRACKED_PAYMENTS:
        _attempts.popitem(last=False)
    return len(attempts)


@app.post("/webhooks")
async def receive(
    request: Request, fail: Literal["always", "never"] = "never", fail_first: int = 0
) -> dict[str, str]:
    body = await request.body()
    expected = sign(body, _secret, request.headers.get(TIMESTAMP_HEADER, ""))
    signature_ok = hmac.compare_digest(request.headers.get(SIGNATURE_HEADER, ""), expected)
    payload = await request.json()
    payment_id = str(payload.get("payment_id"))
    attempt = len(_attempts.get(payment_id, [])) + 1
    respond_ok = fail == "never" and attempt > fail_first
    _record(
        payment_id,
        {
            "received_at": datetime.now(UTC).isoformat(),
            "signature_valid": signature_ok,
            "responded": 200 if respond_ok else 500,
            "payload": payload,
        },
    )
    if not respond_ok:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Simulated client failure")
    return {"status": "received"}


@app.get("/webhooks/{payment_id}")
async def attempts(payment_id: str) -> list[dict[str, Any]]:
    return _attempts.get(payment_id, [])
