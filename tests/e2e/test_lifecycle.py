"""End-to-end against the running compose stack (api + worker + redpanda + postgres).

Runs inside the `tests` compose service; skipped when E2E_API_URL is not set
(e.g. plain local pytest).
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import httpx
import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

API = os.environ.get("E2E_API_URL")
KAFKA = os.environ.get("CP_KAFKA_BOOTSTRAP", "localhost:19092")
DB = os.environ.get("CP_DATABASE_URL", "postgresql://control_plane@localhost:5432/control_plane")

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(not API, reason="E2E_API_URL not set (needs the docker-compose stack)"),
]


@pytest_asyncio.fixture
async def api() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=API or "", timeout=10) as c:
        yield c


@pytest_asyncio.fixture
async def producer() -> AsyncIterator[AIOKafkaProducer]:
    p = AIOKafkaProducer(bootstrap_servers=KAFKA)
    await p.start()
    try:
        yield p
    finally:
        await p.stop()


def unique_slug(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


async def wait_for_status(api: httpx.AsyncClient, tenant_id: str, status: str) -> dict[str, Any]:
    deadline = time.monotonic() + 60
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = (await api.get(f"/v1/tenants/{tenant_id}")).json()
        if last["status"] == status:
            return last
        await asyncio.sleep(0.25)
    raise AssertionError(f"tenant never reached {status!r}; last seen {last}")


async def test_full_lifecycle(api: httpx.AsyncClient) -> None:
    r = await api.post("/v1/tenants", json={"slug": unique_slug("e2e"), "name": "E2E"})
    assert r.status_code == 201, r.text
    tid = r.json()["tenant"]["id"]

    # deploy: provisioning -> active, driven entirely by the worker
    t = await wait_for_status(api, tid, "active")
    assert t["version"] == 2
    tasks = (await api.get(f"/v1/tenants/{tid}/tasks")).json()["items"]
    assert [(x["type"], x["status"]) for x in tasks] == [("deploy", "done")]

    # update: active -> updating -> active
    r = await api.patch(f"/v1/tenants/{tid}", json={"version": t["version"], "name": "E2E v2"})
    assert r.status_code == 202, r.text
    t = await wait_for_status(api, tid, "active")
    assert t["name"] == "E2E v2"
    assert t["version"] == 4

    # destroy: active -> destroying -> destroyed
    assert (await api.delete(f"/v1/tenants/{tid}")).status_code == 202
    t = await wait_for_status(api, tid, "destroyed")
    tasks = (await api.get(f"/v1/tenants/{tid}/tasks")).json()["items"]
    assert [(x["type"], x["status"]) for x in tasks] == [
        ("deploy", "done"),
        ("update", "done"),
        ("destroy", "done"),
    ]


async def wait_for_outcomes(message_ids: list[str]) -> dict[str, str]:
    """Poll the consumer's idempotency ledger until it has handled all `message_ids`."""
    conn = await asyncpg.connect(DB)
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            rows = await conn.fetch(
                "SELECT message_id, outcome FROM processed_messages WHERE message_id = ANY($1)",
                message_ids,
            )
            if len(rows) == len(message_ids):
                return {r["message_id"]: r["outcome"] for r in rows}
            await asyncio.sleep(0.25)
    finally:
        await conn.close()
    raise AssertionError(f"consumer did not process {message_ids}")


async def wait_in_dlq(marker: bytes) -> dict[str, bytes]:
    consumer = AIOKafkaConsumer(
        "tenant.task-updates.dlq",
        bootstrap_servers=KAFKA,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            batch = await consumer.getmany(timeout_ms=500)
            for records in batch.values():
                for r in records:
                    if r.value and marker in r.value:
                        return dict(r.headers)
    finally:
        await consumer.stop()
    raise AssertionError("poison message never reached the DLQ")


async def test_duplicate_stale_and_poison_via_broker(
    api: httpx.AsyncClient, producer: AIOKafkaProducer
) -> None:
    r = await api.post("/v1/tenants", json={"slug": unique_slug("e2e-msg"), "name": "E2E"})
    tid, task_id = r.json()["tenant"]["id"], r.json()["task"]["id"]
    before = await wait_for_status(api, tid, "active")

    def update(status: str, message_id: str) -> bytes:
        return json.dumps(
            {
                "message_id": message_id,
                "task_id": task_id,
                "status": status,
                "occurred_at": "2026-01-01T00:00:00.000Z",
            }
        ).encode()

    stale_ids = [f"manual-{uuid.uuid4()}", f"manual-{uuid.uuid4()}"]
    marker = f"poison-{uuid.uuid4()}".encode()
    for value in (
        update("done", f"{task_id}:done"),  # duplicate of the worker's own report
        update("in_progress", stale_ids[0]),  # out of order
        update("failed", stale_ids[1]),  # contradicts the terminal state
        b"not json " + marker,  # poison
    ):
        await producer.send_and_wait("tenant.task-updates", value=value, key=task_id.encode())

    outcomes = await wait_for_outcomes([f"{task_id}:done", *stale_ids])
    assert outcomes == {f"{task_id}:done": "applied", stale_ids[0]: "stale", stale_ids[1]: "stale"}
    headers = await wait_in_dlq(marker)
    assert headers["dlq_reason"].startswith(b"poison:")

    # None of the injected messages changed the tenant or its task...
    assert (await api.get(f"/v1/tenants/{tid}")).json() == before
    assert (await api.get(f"/v1/tasks/{task_id}")).json()["status"] == "done"

    # ...and the consumer survived the poison message: a new tenant still completes.
    r2 = await api.post("/v1/tenants", json={"slug": unique_slug("e2e-after"), "name": "E2E"})
    await wait_for_status(api, r2.json()["tenant"]["id"], "active")
