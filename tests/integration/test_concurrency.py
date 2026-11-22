"""Race conditions, exercised for real: N requests fired concurrently at one Postgres.

The API runs in-process but each request gets its own pooled DB connection, so the
requests genuinely race inside Postgres (row locks, unique index), which is where
the guarantees live.
"""

from __future__ import annotations

import asyncio
from collections import Counter

import asyncpg
import httpx
import pytest

from tests.conftest import create_tenant, force_tenant_status


def codes(responses: list[httpx.Response]) -> Counter[str]:
    return Counter("ok" if r.status_code < 300 else r.json()["error"]["code"] for r in responses)


@pytest.mark.parametrize("n", [2, 20])
async def test_concurrent_patches_same_version_exactly_one_wins(
    client: httpx.AsyncClient, pool: asyncpg.Pool, n: int
) -> None:
    t = await create_tenant(client, f"race-patch-{n}")
    tid = t["tenant"]["id"]
    v = await force_tenant_status(pool, tid, "active")

    rs = await asyncio.gather(
        *(
            client.patch(f"/v1/tenants/{tid}", json={"version": v, "name": f"n{i}"})
            for i in range(n)
        )
    )

    assert codes(list(rs)) == Counter({"ok": 1, "tenant_version_conflict": n - 1})
    winner = next(r.json() for r in rs if r.status_code == 202)
    row = await pool.fetchrow("SELECT name, version, status FROM tenants WHERE id = $1::uuid", tid)
    assert row is not None
    assert row["version"] == v + 1  # incremented exactly once: no lost update
    assert row["name"] == winner["tenant"]["name"]
    assert row["status"] == "updating"
    open_tasks = await pool.fetchval(
        "SELECT count(*) FROM tasks"
        " WHERE tenant_id=$1::uuid AND status IN ('accepted','in_progress')",
        tid,
    )
    assert open_tasks == 1
    assert (
        await pool.fetchval("SELECT count(*) FROM outbox WHERE payload->'task'->>'type'='update'")
        == 1
    )


async def test_concurrent_creates_same_slug_exactly_one_wins(
    client: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    n = 25
    rs = await asyncio.gather(
        *(client.post("/v1/tenants", json={"slug": "same-slug", "name": f"n{i}"}) for i in range(n))
    )
    assert codes(list(rs)) == Counter({"ok": 1, "tenant_already_exists": n - 1})
    assert await pool.fetchval("SELECT count(*) FROM tenants") == 1
    assert await pool.fetchval("SELECT count(*) FROM tasks") == 1
    assert await pool.fetchval("SELECT count(*) FROM outbox") == 1


async def test_concurrent_deletes_exactly_one_wins(
    client: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    t = await create_tenant(client, "race-delete")
    tid = t["tenant"]["id"]
    await force_tenant_status(pool, tid, "active")
    rs = await asyncio.gather(*(client.delete(f"/v1/tenants/{tid}") for _ in range(10)))
    # Without If-Match the losers see status=destroying -> update not allowed.
    assert codes(list(rs)) == Counter({"ok": 1, "tenant_update_not_allowed": 9})
    assert await pool.fetchval("SELECT count(*) FROM tasks WHERE type='destroy'") == 1


async def test_patch_vs_delete_race(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    """A PATCH and a DELETE racing on an active tenant: exactly one mutation happens."""
    for i in range(10):
        t = await create_tenant(client, f"pd-race-{i}")
        tid = t["tenant"]["id"]
        v = await force_tenant_status(pool, tid, "active")
        p, d = await asyncio.gather(
            client.patch(f"/v1/tenants/{tid}", json={"version": v, "name": "x"}),
            client.delete(f"/v1/tenants/{tid}", headers={"If-Match": str(v)}),
        )
        assert sorted([p.status_code, d.status_code]) == [202, 409]
        row = await pool.fetchrow("SELECT version FROM tenants WHERE id=$1::uuid", tid)
        assert row is not None
        assert row["version"] == v + 1
