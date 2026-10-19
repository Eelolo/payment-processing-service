import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.api.deps import SessionDep, verify_api_key
from app.db.models import IDEMPOTENCY_KEY_MAX_LENGTH
from app.schemas import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentDetail
from app.services import payments as payment_service

IDEMPOTENT_REPLAY_HEADER = "Idempotent-Replayed"

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
    responses={status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={
        status.HTTP_409_CONFLICT: {
            "model": ErrorResponse,
            "description": "Idempotency-Key was already used with a different request body",
        }
    },
)
async def create_payment(
    body: PaymentCreate,
    session: SessionDep,
    response: Response,
    idempotency_key: Annotated[
        # At least one non-whitespace character: a blank key would deduplicate nothing useful.
        str, Header(max_length=IDEMPOTENCY_KEY_MAX_LENGTH, pattern=r"\S")
    ],
) -> PaymentAccepted:
    """Accept a payment for asynchronous processing.

    Repeating the request with the same Idempotency-Key and body returns the original
    payment (with the `Idempotent-Replayed: true` header) and does not create a new one.
    """
    try:
        result = await payment_service.create_payment(session, body, idempotency_key)
    except payment_service.IdempotencyKeyReusedError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from None
    if not result.created:
        response.headers[IDEMPOTENT_REPLAY_HEADER] = "true"
    return PaymentAccepted.model_validate(result.payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentDetail,
    responses={status.HTTP_404_NOT_FOUND: {"model": ErrorResponse}},
)
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentDetail:
    payment = await payment_service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentDetail.model_validate(payment)
