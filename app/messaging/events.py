import uuid

from pydantic import BaseModel

PAYMENT_AGGREGATE = "payment"
PAYMENT_CREATED = "payment.created"

# Delivery attempt number of a message, starting from 1. Set by the consumer on retries.
ATTEMPT_HEADER = "x-attempt"
LAST_ERROR_HEADER = "x-last-error"
# Error messages go into AMQP headers, which are not meant for long text.
MAX_ERROR_HEADER_LENGTH = 500


class PaymentCreatedEvent(BaseModel):
    """Message body of payments.new.

    Carries only the id: the database is the source of truth, and the consumer re-reads
    the payment so a stale or replayed message cannot override the current state.
    """

    payment_id: uuid.UUID
