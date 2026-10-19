"""End-to-end scenarios against the running docker compose stack (`make up`, then `make e2e`).

Standard library only, so it runs with any python3 on the host.
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

API_URL = os.environ.get("API_URL", "http://localhost:8000")
API_KEY = os.environ.get("API_KEY", "local-dev-api-key")
SINK_URL = os.environ.get("SINK_URL", "http://localhost:8080")
RABBITMQ_API = os.environ.get("RABBITMQ_API", "http://localhost:15672/api")
# Address of the demo webhook receiver as seen from the consumer container.
SINK_IN_NETWORK = "http://webhook-sink:8080/webhooks"

# Gateway takes up to 5s; retries add 1s + 2s with the default settings.
PROCESSING_TIMEOUT_S = 20
BATCH_SIZE = 20


def request(
    method: str, url: str, body: Any = None, headers: dict[str, str] | None = None
) -> tuple[int, dict[str, str], Any]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})  # noqa: S310
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
            return resp.status, dict(resp.headers), json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers), json.loads(err.read() or b"null")


def api(
    method: str, path: str, body: Any = None, **headers: str
) -> tuple[int, dict[str, str], Any]:
    return request(method, API_URL + path, body, {"X-API-Key": API_KEY, **headers})


def create(webhook_query: str = "", key: str | None = None, amount: str = "100.00") -> Any:
    body = {
        "amount": amount,
        "currency": "RUB",
        "description": "e2e",
        "metadata": {"scenario": webhook_query or "happy"},
        "webhook_url": SINK_IN_NETWORK + webhook_query,
    }
    status, _, payload = api(
        "POST", "/api/v1/payments", body, **{"Idempotency-Key": key or str(uuid.uuid4())}
    )
    assert status == 202, (status, payload)
    return payload


def wait_until(check: Callable[[], Any], what: str, timeout: float = PROCESSING_TIMEOUT_S) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := check():
            return result
        time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for {what}")


def payment(payment_id: str) -> Any:
    return api("GET", f"/api/v1/payments/{payment_id}")[2]


def sink_attempts(payment_id: str) -> Any:
    return request("GET", f"{SINK_URL}/webhooks/{payment_id}")[2]


def dlq_size() -> int:
    auth = base64.b64encode(b"guest:guest").decode()
    _, _, queue = request(
        "GET",
        f"{RABBITMQ_API}/queues/%2F/payments.new.dlq",
        headers={"Authorization": f"Basic {auth}"},
    )
    return int(queue.get("messages", 0))


def scenario_auth() -> None:
    status, _, _ = request("GET", f"{API_URL}/api/v1/payments/{uuid.uuid4()}")
    assert status == 401, status
    status, _, _ = request(
        "GET", f"{API_URL}/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "wrong"}
    )
    assert status == 401, status


def scenario_idempotency() -> None:
    key = str(uuid.uuid4())
    first = create(key=key)
    status, headers, replay = api(
        "POST",
        "/api/v1/payments",
        {
            "amount": "100",
            "currency": "RUB",
            "description": "e2e",
            "metadata": {"scenario": "happy"},
            "webhook_url": SINK_IN_NETWORK,
        },
        **{"Idempotency-Key": key},
    )
    assert status == 202 and replay["payment_id"] == first["payment_id"], replay
    assert headers.get("idempotent-replayed") == "true", headers
    status, _, _ = api(
        "POST",
        "/api/v1/payments",
        {"amount": "1", "currency": "RUB", "description": "x", "webhook_url": SINK_IN_NETWORK},
        **{"Idempotency-Key": key},
    )
    assert status == 409, status


def scenario_happy_path() -> None:
    created = create()
    final = wait_until(
        lambda: (p := payment(created["payment_id"]))["webhook_delivered_at"] and p,
        "webhook delivery",
    )
    assert final["status"] in ("succeeded", "failed") and final["processed_at"], final
    (attempt,) = sink_attempts(created["payment_id"])
    assert attempt["signature_valid"] and attempt["payload"]["status"] == final["status"], attempt
    print(f"    payment {final['payment_id']} -> {final['status']}, webhook signed and delivered")


def scenario_retry_then_success() -> None:
    created = create("?fail_first=2")
    wait_until(
        lambda: payment(created["payment_id"])["webhook_delivered_at"],
        "webhook delivery after retries",
    )
    attempts = sink_attempts(created["payment_id"])
    assert [a["responded"] for a in attempts] == [500, 500, 200], attempts
    print("    webhook answered 500, 500, 200: delivered on the 3rd attempt")


def scenario_dead_letter() -> None:
    before = dlq_size()
    created = create("?fail=always")
    # Management API statistics refresh every few seconds, hence the generous wait.
    wait_until(lambda: dlq_size() > before, "message in DLQ", timeout=PROCESSING_TIMEOUT_S + 10)
    attempts = sink_attempts(created["payment_id"])
    final = payment(created["payment_id"])
    assert len(attempts) == 3 and final["webhook_delivered_at"] is None, (attempts, final)
    print(
        "    3 failed webhook attempts, message in payments.new.dlq, "
        f"payment stays {final['status']}"
    )


def scenario_concurrent_processing() -> None:
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=BATCH_SIZE) as pool:
        ids = [p["payment_id"] for p in pool.map(lambda _: create(), range(BATCH_SIZE))]
    wait_until(
        lambda: all(payment(i)["webhook_delivered_at"] for i in ids), f"{BATCH_SIZE} payments"
    )
    elapsed = time.monotonic() - started
    statuses = [payment(i)["status"] for i in ids]
    print(
        f"    {BATCH_SIZE} payments in {elapsed:.1f}s "
        f"(sequential would be ~{BATCH_SIZE * 3.5:.0f}s): "
        f"{statuses.count('succeeded')} succeeded, {statuses.count('failed')} failed"
    )


SCENARIOS = [
    scenario_auth,
    scenario_idempotency,
    scenario_happy_path,
    scenario_retry_then_success,
    scenario_dead_letter,
    scenario_concurrent_processing,
]


def main() -> int:
    failed = 0
    for scenario in SCENARIOS:
        name = scenario.__name__.removeprefix("scenario_")
        print(f"- {name}")
        try:
            scenario()
        except AssertionError as err:
            failed += 1
            print(f"    FAILED: {err}")
    print("OK" if not failed else f"{failed} scenario(s) failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
