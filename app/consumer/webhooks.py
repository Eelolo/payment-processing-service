import hashlib
import hmac
import time

import httpx

from app.db.models import Payment
from app.schemas import WebhookPayload

SIGNATURE_HEADER = "X-Webhook-Signature"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
EVENT_PREFIX = "payment."


class WebhookDeliveryError(Exception):
    """The client's endpoint did not accept the notification."""


def sign(body: bytes, secret: bytes, timestamp: str) -> str:
    """HMAC-SHA256 over "<timestamp>.<body>".

    The timestamp is signed too, so a client that rejects old timestamps is protected
    from a captured notification being replayed later.
    """
    signed = timestamp.encode() + b"." + body
    return "sha256=" + hmac.new(secret, signed, hashlib.sha256).hexdigest()


class WebhookNotifier:
    """Sends the payment result to the client's webhook_url.

    The body is signed with HMAC-SHA256 so the client can verify it came from us.
    Any non-2xx answer or transport error is a failed delivery; retries are the
    caller's job (they go through the broker, see app.consumer.handler).
    """

    def __init__(self, client: httpx.AsyncClient, secret: bytes) -> None:
        self._client = client
        self._secret = secret

    async def send(self, payment: Payment) -> None:
        payload = WebhookPayload(
            event=EVENT_PREFIX + payment.status.value,
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            metadata=payment.metadata_,
            processed_at=payment.processed_at,
        )
        body = payload.model_dump_json().encode()
        timestamp = str(int(time.time()))
        try:
            response = await self._client.post(
                payment.webhook_url,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    TIMESTAMP_HEADER: timestamp,
                    SIGNATURE_HEADER: sign(body, self._secret, timestamp),
                },
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc
