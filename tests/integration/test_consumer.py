"""Inbound updates: idempotent, order-tolerant, resilient (against a real Postgres)."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from typing import Any
from uuid import uuid4

import asyncpg
import httpx
import pytest

from control_plane.config import Settings
from control_plane.messaging.consumer import (
    MAX_ATTEMPTS_FOR_UNEXPECTED_ERRORS,
    Record,
    RecordProcessor,
)
from control_plane.messaging.handler import Outcome, PoisonMessage, UpdateHandler
from tests.conftest import create_tenant, force_tenant_status
from tests.fakes import FakePublisher


def msg(task_id: str, status: str, message_id: str | None = None, **extra: Any) -> bytes:
    body = {
        "message_id": message_id or f"{task_id}:{status}",
        "task_id": task_id,
        "status": status,
        "occurred_at": "2026-01-01T00:00:00.000Z",
        **extra,
    }
    return json.dumps(body).encode()


async def state(pool: asyncpg.Pool, task_id: str) -> tuple[str, str, int]:
    row = await pool.fetchrow(
        "SELECT t.status AS task, n.status AS tenant, n.version"
        "  FROM tasks t JOIN tenants n ON n.id = t.tenant_id WHERE t.id = $1::uuid",
        task_id,
    )
    assert row is not None
    return row["task"], row["tenant"], row["version"]


@pytest.fixture
def handler(pool: asyncpg.Pool) -> UpdateHandler:
    return UpdateHandler(pool)


# ------------------------------------------------------------------------ lifecycle
async def test_deploy_happy_path(
    client: httpx.AsyncClient, pool: asyncpg.Pool, handler: UpdateHandler
) -> None:
    t = await create_tenant(client, "life")
    task_id = t["task"]["id"]

    assert await handler.handle(msg(task_id, "in_progress")) is Outcome.APPLIED
    assert await state(pool, task_id) == ("in_progress", "provisioning", 1)  # tenant untouched

    assert await handler.handle(msg(task_id, "done")) is Outcome.APPLIED
    assert await state(pool, task_id) == ("done", "active", 2)  # worker change bumps version

    # Visible through the API too.
    r = await client.get(f"/v1/tenants/{t['tenant']['id']}")
    assert r.json()["status"] == "active"
    assert r.json()["version"] == 2


@pytest.mark.parametrize(
    ("task_type", "start_status", "outcome", "end_status"),
    [
        ("deploy", None, "failed", "failed"),
        ("update", "active", "done", "active"),
        ("update", "active", "failed", "failed"),
        ("destroy", "active", "done", "destroyed"),
        ("destroy", "failed", "done", "destroyed"),
        ("destroy", "active", "failed", "failed"),
    ],
)
async def test_terminal_outcomes_move_tenant(
    client: httpx.AsyncClient,
    pool: asyncpg.Pool,
    handler: UpdateHandler,
    task_type: str,
    start_status: str | None,
    outcome: str,
    end_status: str,
) -> None:
    t = await create_tenant(client, f"t-{task_type}-{outcome}-{start_status or 'x'}")
    tid = t["tenant"]["id"]
    task_id = t["task"]["id"]
    if task_type != "deploy":
        assert start_status is not None
        v = await force_tenant_status(pool, tid, start_status)
        r = (
            await client.patch(f"/v1/tenants/{tid}", json={"version": v, "name": "n"})
            if task_type == "update"
            else await client.delete(f"/v1/tenants/{tid}")
        )
        task_id = r.json()["task"]["id"]

    await handler.handle(msg(task_id, "in_progress"))
    await handler.handle(msg(task_id, outcome, error="boom" if outcome == "failed" else None))
    task_status, tenant_status, _ = await state(pool, task_id)
    assert (task_status, tenant_status) == (outcome, end_status)
    if outcome == "failed":
        assert (await client.get(f"/v1/tasks/{task_id}")).json()["error"] == "boom"


# ---------------------------------------------------------------------- idempotency
async def test_duplicate_delivery_changes_nothing(
    client: httpx.AsyncClient, pool: asyncpg.Pool, handler: UpdateHandler
) -> None:
    t = await create_tenant(client, "dupe")
    task_id = t["task"]["id"]
    done = msg(task_id, "done", message_id="m-1")

    assert await handler.handle(done) is Outcome.APPLIED
    before = await state(pool, task_id)
    for _ in range(3):
        assert await handler.handle(done) is Outcome.DUPLICATE
    assert await state(pool, task_id) == before  # version not bumped again


async def test_concurrent_duplicate_delivery_applies_once(
    client: httpx.AsyncClient, pool: asyncpg.Pool, handler: UpdateHandler
) -> None:
    """Same message delivered 10x in parallel (e.g. two consumers after a rebalance)."""
    t = await create_tenant(client, "dupe-race")
    task_id = t["task"]["id"]
    done = msg(task_id, "done", message_id="race-1")
    outcomes = await asyncio.gather(*(handler.handle(done) for _ in range(10)))
    assert Counter(outcomes) == Counter({Outcome.APPLIED: 1, Outcome.DUPLICATE: 9})
    assert await state(pool, task_id) == ("done", "active", 2)


# --------------------------------------------------------------------- order tolerance
async def test_in_progress_after_done_is_stale(
    client: httpx.AsyncClient, pool: asyncpg.Pool, handler: UpdateHandler
) -> None:
    t = await create_tenant(client, "late-progress")
    task_id = t["task"]["id"]
    assert await handler.handle(msg(task_id, "done")) is Outcome.APPLIED  # skipped in_progress
    assert await handler.handle(msg(task_id, "in_progress")) is Outcome.STALE
    assert await state(pool, task_id) == ("done", "active", 2)


@pytest.mark.parametrize(("first", "second"), [("done", "failed"), ("failed", "done")])
async def test_terminal_never_flips(
    client: httpx.AsyncClient,
    pool: asyncpg.Pool,
    handler: UpdateHandler,
    first: str,
    second: str,
) -> None:
    t = await create_tenant(client, f"flip-{first}")
    task_id = t["task"]["id"]
    await handler.handle(msg(task_id, first))
    snapshot = await state(pool, task_id)
    assert await handler.handle(msg(task_id, second)) is Outcome.STALE
    assert await state(pool, task_id) == snapshot


async def test_stale_outcome_is_recorded(
    client: httpx.AsyncClient, pool: asyncpg.Pool, handler: UpdateHandler
) -> None:
    t = await create_tenant(client, "audit")
    task_id = t["task"]["id"]
    await handler.handle(msg(task_id, "done", message_id="a"))
    await handler.handle(msg(task_id, "in_progress", message_id="b"))
    rows = await pool.fetch("SELECT message_id, outcome FROM processed_messages ORDER BY 1")
    assert [(r["message_id"], r["outcome"]) for r in rows] == [("a", "applied"), ("b", "stale")]


# -------------------------------------------------------------------------- poison
@pytest.mark.parametrize(
    "raw",
    [
        None,
        b"",
        b"not json",
        b"\xff\xfe",
        b"[]",
        json.dumps({"task_id": str(uuid4()), "status": "done"}).encode(),  # no message_id
        msg("not-a-uuid", "done"),
        msg(str(uuid4()), "accepted"),  # workers may not report `accepted`
        msg(str(uuid4()), "exploded"),
        msg(str(uuid4()), "done"),  # unknown task
    ],
)
async def test_poison_messages_are_rejected(handler: UpdateHandler, raw: bytes | None) -> None:
    with pytest.raises(PoisonMessage):
        await handler.handle(raw)


# ----------------------------------------------------------------- record processor
def processor(pool: asyncpg.Pool, settings: Settings, dlq: FakePublisher) -> RecordProcessor:
    return RecordProcessor(UpdateHandler(pool), dlq, settings)


def record(value: bytes | None, offset: int = 7) -> Record:
    return Record("tenant.task-updates", 0, offset, b"k", value)


async def test_poison_goes_to_dlq(pool: asyncpg.Pool, settings: Settings) -> None:
    dlq = FakePublisher(fail_next=2)  # DLQ itself is briefly down: retried, not dropped
    out = await processor(pool, settings, dlq).process(record(b"garbage"))
    assert out is None
    assert len(dlq.sent) == 1
    sent = dlq.sent[0]
    assert sent.topic == "tenant.task-updates.dlq"
    assert sent.value == b"garbage"
    assert sent.header("dlq_reason").startswith("poison:")
    assert sent.header("source_offset") == "7"


async def test_transient_db_error_is_retried_until_success(
    client: httpx.AsyncClient,
    pool: asyncpg.Pool,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    t = await create_tenant(client, "transient")
    real = UpdateHandler.handle
    failures = {"left": 3}

    async def flaky(self: UpdateHandler, raw: bytes | None) -> Outcome:
        if failures["left"]:
            failures["left"] -= 1
            raise ConnectionResetError("db connection lost (simulated)")
        return await real(self, raw)

    monkeypatch.setattr(UpdateHandler, "handle", flaky)
    dlq = FakePublisher()
    out = await processor(pool, settings, dlq).process(record(msg(t["task"]["id"], "done")))
    assert out is Outcome.APPLIED
    assert dlq.sent == []


async def test_unexpected_error_is_dead_lettered_after_max_attempts(
    pool: asyncpg.Pool, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}

    async def buggy(self: UpdateHandler, raw: bytes | None) -> Outcome:
        calls["n"] += 1
        raise KeyError("bug")

    monkeypatch.setattr(UpdateHandler, "handle", buggy)
    dlq = FakePublisher()
    assert await processor(pool, settings, dlq).process(record(b"{}")) is None
    assert calls["n"] == MAX_ATTEMPTS_FOR_UNEXPECTED_ERRORS
    assert dlq.sent[0].header("dlq_reason").startswith("unexpected: KeyError")


async def test_shuffled_and_duplicated_stream_converges(
    client: httpx.AsyncClient, pool: asyncpg.Pool, settings: Settings
) -> None:
    """A messy stream for many tenants (dupes + out-of-order) ends in the right state."""
    import random

    rng = random.Random(42)
    tasks = [(await create_tenant(client, f"mess-{i}"))["task"]["id"] for i in range(15)]
    stream: list[bytes] = []
    for i, task_id in enumerate(tasks):
        final = "failed" if i % 3 == 0 else "done"
        stream += [msg(task_id, "in_progress"), msg(task_id, final)] * 2  # every msg twice
    rng.shuffle(stream)

    p = processor(pool, settings, FakePublisher())
    for n, raw in enumerate(stream):
        await p.process(record(raw, offset=n))

    for i, task_id in enumerate(tasks):
        expected = ("failed", "failed") if i % 3 == 0 else ("done", "active")
        task_status, tenant_status, version = await state(pool, task_id)
        assert (task_status, tenant_status) == expected
        assert version == 2  # exactly one tenant mutation from the worker
