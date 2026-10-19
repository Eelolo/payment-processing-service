import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator

from app.db.models import (
    AMOUNT_PRECISION,
    AMOUNT_SCALE,
    WEBHOOK_URL_MAX_LENGTH,
    Currency,
    PaymentStatus,
)

Amount = Annotated[
    Decimal,
    Field(gt=0, max_digits=AMOUNT_PRECISION, decimal_places=AMOUNT_SCALE, examples=["1500.00"]),
]
AMOUNT_QUANTUM = Decimal(1).scaleb(-AMOUNT_SCALE)
# metadata is free-form client data stored in every row and every webhook: keep it small.
MAX_METADATA_BYTES = 16 * 1024


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: AnyHttpUrl = Field(max_length=WEBHOOK_URL_MAX_LENGTH)

    @field_validator("metadata")
    @classmethod
    def limit_metadata_size(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value).encode()) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata must not exceed {MAX_METADATA_BYTES} bytes")
        return value

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        # 100, 100.0 and 100.00 are the same amount and must produce the same request hash.
        return value.quantize(AMOUNT_QUANTUM)


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None


class WebhookPayload(BaseModel):
    """Body of the result notification sent to the client's webhook_url."""

    model_config = ConfigDict(from_attributes=True)

    event: str
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    metadata: dict[str, Any]
    processed_at: datetime | None


class ErrorResponse(BaseModel):
    detail: str
