"""Outbox relay: publish only after commit, at-least-once, safe with N relays."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import asyncpg
import httpx
import pytest

from control_plane.config import Settings
from control_plane.messaging.contracts import TaskEvent
from control_plane.messaging.relay import OutboxRelay
from tests.conftest import create_tenant
from tests.fakes import FakePublisher


def relay(
    pool: asyncpg.Pool, settings: Settings, pub: FakePublisher, batch: int = 100
) -> OutboxRelay:
    return OutboxRelay(
        pool, settings.model_copy(update={"outbox_batch_size": batch}), publisher=pub
    )


async def test_publishes_task_event_and_marks_row(
    client: httpx.AsyncClient, pool: asyncpg.Pool, settings: Settings
) -> None:
    t = await create_tenant(client, "relay-one")
    pub = FakePublisher()

    assert await relay(pool, settings, pub).publish_batch() == 1
    assert len(pub.sent) == 1
    sent = pub.sent[0]
    assert sent.topic == "tenant.tasks"
    assert sent.key == t["tenant"]["id"]  # partition key => per-tenant ordering
    assert sent.header("event_id") == t["task"]["id"]

    # The consumer can recover id, type, tenant_id and status from the event.
    event = TaskEvent.model_validate(sent.json)
    assert str(event.event_id) == t["task"]["id"]
    assert str(event.task.id) == t["task"]["id"]
    assert event.task.type == "deploy"
    assert str(event.task.tenant_id) == t["tenant"]["id"]
    assert event.task.status == "accepted"
    assert event.tenant.slug == "relay-one"

    assert await pool.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 0
    assert await relay(pool, settings, pub).publish_batch() == 0  # nothing left
    assert len(pub.sent) == 1


async def test_uncommitted_outbox_rows_are_invisible(
    pool: asyncpg.Pool, settings: Settings
) -> None:
    """The 'never before commit' guarantee: an open transaction's event is not published."""
    pub = FakePublisher()
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        await conn.execute(
            "INSERT INTO outbox (event_id, topic, key, payload) VALUES ($1, 't', 'k', '{}'::jsonb)",
            uuid4(),
        )
        assert await relay(pool, settings, pub).publish_batch() == 0  # not committed yet
        await tx.rollback()
    assert await relay(pool, settings, pub).publish_batch() == 0  # rolled back: never
    assert pub.sent == []


async def test_broker_failure_rolls_back_and_retries(
    client: httpx.AsyncClient, pool: asyncpg.Pool, settings: Settings
) -> None:
    for i in range(3):
        await create_tenant(client, f"relay-fail-{i}")
    pub = FakePublisher()
    pub.fail_next = 1  # first publish fails mid-batch

    with pytest.raises(ConnectionError):
        await relay(pool, settings, pub).publish_batch()
    assert await pool.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 3

    assert await relay(pool, settings, pub).publish_batch() == 3
    assert await pool.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL") == 0


async def test_partial_batch_failure_is_at_least_once(
    client: httpx.AsyncClient, pool: asyncpg.Pool, settings: Settings
) -> None:
    """Event 1 is acked, event 2 fails -> batch retried -> event 1 is sent twice.
    That duplicate is the documented price of at-least-once; consumers dedupe by event_id."""
    await create_tenant(client, "relay-dup-a")
    await create_tenant(client, "relay-dup-b")

    class FailSecond(FakePublisher):
        calls = 0

        async def publish(self, topic: str, **kw: object) -> None:  # type: ignore[override]
            FailSecond.calls += 1
            if FailSecond.calls == 2:
                raise ConnectionError("boom")
            await super().publish(topic, **kw)  # type: ignore[arg-type]

    pub = FailSecond()
    with pytest.raises(ConnectionError):
        await relay(pool, settings, pub).publish_batch()
    await relay(pool, settings, pub).publish_batch()
    ids = [s.header("event_id") for s in pub.sent]
    assert len(ids) == 3
    assert len(set(ids)) == 2


async def test_preserves_creation_order(
    client: httpx.AsyncClient, pool: asyncpg.Pool, settings: Settings
) -> None:
    tasks = [(await create_tenant(client, f"order-{i}"))["task"]["id"] for i in range(7)]
    pub = FakePublisher()
    r = relay(pool, settings, pub, batch=3)
    while await r.publish_batch():
        pass
    assert [s.header("event_id") for s in pub.sent] == tasks


async def test_concurrent_relays_never_double_publish(
    client: httpx.AsyncClient, pool: asyncpg.Pool, settings: Settings
) -> None:
    """Two relay replicas draining at once: SKIP LOCKED gives each a disjoint set of rows."""
    for i in range(30):
        await create_tenant(client, f"multi-relay-{i}")
    pub = FakePublisher(delay_s=0.001)
    r1, r2 = relay(pool, settings, pub, batch=5), relay(pool, settings, pub, batch=5)

    async def drain(r: OutboxRelay) -> None:
        while await r.publish_batch():
            pass

    await asyncio.gather(drain(r1), drain(r2))
    ids = [s.header("event_id") for s in pub.sent]
    assert len(ids) == 30
    assert len(set(ids)) == 30


async def test_run_loop_survives_outage(
    client: httpx.AsyncClient, pool: asyncpg.Pool, settings: Settings
) -> None:
    await create_tenant(client, "relay-loop")
    pub = FakePublisher(fail_next=3)
    task = asyncio.create_task(relay(pool, settings, pub).run())
    try:
        for _ in range(200):
            if pub.sent:
                break
            await asyncio.sleep(0.01)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(pub.sent) == 1
