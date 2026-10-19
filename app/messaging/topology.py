"""RabbitMQ topology: one place that defines every exchange, queue and binding.

    payments (direct) --payments.new--> [payments.new] (quorum) --> consumer
                                            |  reject / attempts exhausted / delivery limit
                                            v
    payments.dlx (direct) --payments.new.dlq--> [payments.new.dlq]

    payments.retry (direct) --<queue name>--> [payments.new.retry.<delay>ms]
                                                   | TTL = delay, no consumers
                                                   | TTL expired: dead-lettered back to
                                                   v
                                    payments --payments.new--> [payments.new]

Retries use plain TTL + dead-lettering, so no broker plugin is needed. There is one delay
queue per backoff step instead of a single queue with per-message TTL: RabbitMQ only expires
messages at the head of a queue, so with mixed TTLs a 4s message would block a 1s one behind it.
The delay is part of the queue name because queue arguments cannot change after declaration:
a new RETRY_BASE_DELAY_MS gives new queues instead of a PRECONDITION_FAILED error.
"""

from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_NEW_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.new.dlq"
# Redeliveries without an ack (the consumer crashed or was killed mid-message) are counted
# by the broker; past this limit the message is dead-lettered instead of crashing it forever.
MAX_UNACKED_DELIVERIES = 5

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)

# Quorum queue: replicated, and unlike a classic queue it supports a delivery limit.
PAYMENTS_NEW_QUEUE = RabbitQueue(
    "payments.new",
    queue_type=QueueType.QUORUM,
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        # Anything the consumer rejects (e.g. a message that cannot be decoded) lands in the DLQ.
        "x-dead-letter-exchange": DLX_EXCHANGE.name,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
        "x-delivery-limit": MAX_UNACKED_DELIVERIES,
    },
)

PAYMENTS_DLQ = RabbitQueue("payments.new.dlq", durable=True, routing_key=DLQ_ROUTING_KEY)


def retry_queue_name(delay_ms: int) -> str:
    return f"payments.new.retry.{delay_ms}ms"


def retry_queue(delay_ms: int) -> RabbitQueue:
    name = retry_queue_name(delay_ms)
    return RabbitQueue(
        name,
        durable=True,
        routing_key=name,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
        },
    )


async def declare_topology(broker: RabbitBroker, retry_delays_ms: list[int]) -> None:
    """Idempotently declare the whole topology.

    Both the outbox relay and the consumer call this on startup: if the relay published
    before the main queue existed, the message would be unroutable.
    """
    bindings = [
        (PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE),
        (DLX_EXCHANGE, PAYMENTS_DLQ),
        *((RETRY_EXCHANGE, retry_queue(delay)) for delay in retry_delays_ms),
    ]
    for exchange, queue in bindings:
        declared_exchange = await broker.declare_exchange(exchange)
        declared_queue = await broker.declare_queue(queue)
        await declared_queue.bind(declared_exchange, routing_key=queue.routing())
