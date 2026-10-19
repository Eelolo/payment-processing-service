from functools import lru_cache
from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """The part of the configuration that migrations need: running them requires no secrets."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    db_pool_size: int = 10


class Settings(DatabaseSettings):
    """Service configuration. Every value can be overridden by an env variable of the same name."""

    # No default on purpose: the service must not start with a guessable key.
    api_key: SecretStr

    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    log_level: str = "INFO"

    # Payment gateway emulation, values from the task: 2-5 seconds, 90% success.
    gateway_min_delay_s: float = 2.0
    gateway_max_delay_s: float = 5.0
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    # A hung webhook endpoint must not hold a consumer slot indefinitely.
    webhook_timeout_s: float = 5.0
    # HMAC key for the X-Webhook-Signature header, required for the same reason as api_key.
    webhook_secret: SecretStr

    # Total delivery attempts per message (first try + retries) before it goes to the DLQ.
    max_attempts: int = Field(default=3, ge=1)
    # Delay before retry N is retry_base_delay_ms * 2 ** (N - 1): 1s, 2s, 4s, ...
    retry_base_delay_ms: int = Field(default=1000, ge=1)
    # Processing is I/O bound (2-5s gateway call), so the consumer handles messages
    # concurrently; prefetch bounds that concurrency.
    consumer_prefetch: int = Field(default=20, ge=1)

    # Upper bound for a publish + confirm. Without it a blocked broker (memory or disk alarm)
    # would hang the relay inside an open transaction that holds outbox row locks.
    broker_publish_timeout_s: float = 10.0
    # Graceful shutdown waits for in-flight messages: gateway (up to 5s) + webhook timeout (5s).
    consumer_graceful_timeout_s: float = 15.0

    outbox_poll_interval_s: float = 1.0
    outbox_batch_size: int = Field(default=100, ge=1)
    # Backoff cap for the relay when the broker is unavailable.
    outbox_max_backoff_s: float = 30.0

    @model_validator(mode="after")
    def check_gateway_delays(self) -> Self:
        if self.gateway_min_delay_s > self.gateway_max_delay_s:
            raise ValueError("GATEWAY_MIN_DELAY_S must not exceed GATEWAY_MAX_DELAY_S")
        return self

    @property
    def retry_delays_ms(self) -> list[int]:
        """Delay before each retry, in order. Length is max_attempts - 1."""
        return [self.retry_base_delay_ms * 2**i for i in range(self.max_attempts - 1)]


@lru_cache
def get_settings() -> Settings:
    return Settings()  # required values come from the environment
