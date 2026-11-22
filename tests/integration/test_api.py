"""HTTP API behaviour against a real Postgres: state guards, errors, pagination, outbox."""

from __future__ import annotations

import re
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest

from control_plane.service import TenantService
from tests.conftest import create_tenant, force_tenant_status

ISO_MS_Z = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")


def assert_error(r: httpx.Response, status: int, code: str) -> None:
    assert r.status_code == status, r.text
    body = r.json()
    assert set(body) == {"error"}
    assert body["error"]["code"] == code
    assert isinstance(body["error"]["message"], str)
    assert isinstance(body["error"]["details"], dict)


# ---------------------------------------------------------------------------- create
async def test_create_tenant_starts_deploy_task_and_writes_outbox(
    client: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    r = await client.post("/v1/tenants", json={"slug": "acme-prod", "name": "Acme"})
    assert r.status_code == 201
    body = r.json()
    tenant, task = body["tenant"], body["task"]
    assert r.headers["Location"] == f"/v1/tenants/{tenant['id']}"
    assert tenant["status"] == "provisioning"
    assert tenant["version"] == 1
    assert ISO_MS_Z.match(tenant["created_at"])
    assert ISO_MS_Z.match(tenant["updated_at"])
    assert task["type"] == "deploy"
    assert task["status"] == "accepted"
    assert task["tenant_id"] == tenant["id"]

    rows = await pool.fetch("SELECT * FROM outbox")
    assert len(rows) == 1
    ob = rows[0]
    assert str(ob["event_id"]) == task["id"]
    assert ob["key"] == tenant["id"]
    assert ob["published_at"] is None
    # The event carries everything needed to recover the task.
    assert ob["payload"]["task"] == {
        "id": task["id"],
        "type": "deploy",
        "tenant_id": tenant["id"],
        "status": "accepted",
        "created_at": ob["payload"]["task"]["created_at"],
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"slug": "ab", "name": "x"},
        {"slug": "Acme", "name": "x"},
        {"slug": "acme-", "name": "x"},
        {"slug": "acme\n", "name": "x"},
        {"slug": "acme", "name": ""},
        {"slug": "acme", "name": "   "},
        {"slug": "acme"},
        {"slug": "acme", "name": "x", "status": "active"},  # unknown field
    ],
)
async def test_create_validation(client: httpx.AsyncClient, payload: dict[str, str]) -> None:
    assert_error(await client.post("/v1/tenants", json=payload), 422, "validation_error")


async def test_duplicate_slug(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    await create_tenant(client, "dup-slug")
    assert_error(
        await client.post("/v1/tenants", json={"slug": "dup-slug", "name": "x"}),
        409,
        "tenant_already_exists",
    )
    assert await pool.fetchval("SELECT count(*) FROM outbox") == 1  # loser published nothing


async def test_slug_stays_reserved_after_destroy(
    client: httpx.AsyncClient, pool: asyncpg.Pool
) -> None:
    t = await create_tenant(client, "gone")
    await force_tenant_status(pool, t["tenant"]["id"], "destroyed")
    assert_error(
        await client.post("/v1/tenants", json={"slug": "gone", "name": "x"}),
        409,
        "tenant_already_exists",
    )


async def test_rollback_publishes_nothing(
    client: httpx.AsyncClient, pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If anything fails after the outbox row is written, the whole transaction rolls back:
    no tenant, no task, no event."""
    original = TenantService._start_task

    async def boom(self: TenantService, *args: object, **kwargs: object) -> object:
        await original(self, *args, **kwargs)  # type: ignore[arg-type]
        raise RuntimeError("crash after outbox insert")

    monkeypatch.setattr(TenantService, "_start_task", boom)
    r = await client.post("/v1/tenants", json={"slug": "rolled-back", "name": "x"})
    assert_error(r, 500, "internal_error")
    assert "Traceback" not in r.text
    for table in ("tenants", "tasks", "outbox"):
        assert await pool.fetchval(f"SELECT count(*) FROM {table}") == 0  # noqa: S608


# ------------------------------------------------------------------------------- get
async def test_get_tenant(client: httpx.AsyncClient) -> None:
    t = await create_tenant(client, "get-me")
    r = await client.get(f"/v1/tenants/{t['tenant']['id']}")
    assert r.status_code == 200
    assert r.json() == t["tenant"]


async def test_not_found_errors(client: httpx.AsyncClient) -> None:
    assert_error(await client.get(f"/v1/tenants/{uuid4()}"), 404, "tenant_not_found")
    assert_error(await client.get(f"/v1/tasks/{uuid4()}"), 404, "task_not_found")
    assert_error(await client.get(f"/v1/tenants/{uuid4()}/tasks"), 404, "tenant_not_found")
    assert_error(
        await client.patch(f"/v1/tenants/{uuid4()}", json={"version": 1, "name": "x"}),
        404,
        "tenant_not_found",
    )
    assert_error(await client.delete(f"/v1/tenants/{uuid4()}"), 404, "tenant_not_found")
    assert_error(await client.get("/v1/tenants/not-a-uuid"), 422, "validation_error")
    assert_error(await client.get("/v1/nope"), 404, "not_found")
    assert_error(await client.put("/v1/tasks"), 405, "method_not_allowed")


# ----------------------------------------------------------------------------- patch
async def test_patch_happy_path(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    t = await create_tenant(client, "patch-me")
    tid = t["tenant"]["id"]
    v = await force_tenant_status(pool, tid, "active")

    r = await client.patch(f"/v1/tenants/{tid}", json={"version": v, "name": "Renamed"})
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["tenant"]["status"] == "updating"
    assert body["tenant"]["name"] == "Renamed"
    assert body["tenant"]["version"] == v + 1
    assert body["task"]["type"] == "update"
    assert body["task"]["status"] == "accepted"
    assert await pool.fetchval("SELECT count(*) FROM outbox") == 2  # deploy + update


@pytest.mark.parametrize(
    "status", ["provisioning", "updating", "destroying", "destroyed", "failed"]
)
async def test_patch_not_allowed_outside_active(
    client: httpx.AsyncClient, pool: asyncpg.Pool, status: str
) -> None:
    t = await create_tenant(client, f"p-{status}")
    tid = t["tenant"]["id"]
    v = await force_tenant_status(pool, tid, status)
    r = await client.patch(f"/v1/tenants/{tid}", json={"version": v, "name": "x"})
    assert_error(r, 409, "tenant_update_not_allowed")
    assert r.json()["error"]["details"]["current_status"] == status


async def test_patch_stale_version(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    t = await create_tenant(client, "stale")
    tid = t["tenant"]["id"]
    v = await force_tenant_status(pool, tid, "active")
    r = await client.patch(f"/v1/tenants/{tid}", json={"version": v - 1, "name": "x"})
    assert_error(r, 409, "tenant_version_conflict")
    assert r.json()["error"]["details"] == {"expected_version": v - 1, "current_version": v}


@pytest.mark.parametrize(
    "payload",
    [{"name": "x"}, {"version": 1}, {"version": 0, "name": "x"}, {"version": 1, "slug": "other"}],
)
async def test_patch_validation(client: httpx.AsyncClient, payload: dict[str, object]) -> None:
    t = await create_tenant(client, "patch-val")
    assert_error(
        await client.patch(f"/v1/tenants/{t['tenant']['id']}", json=payload),
        422,
        "validation_error",
    )


# ---------------------------------------------------------------------------- delete
@pytest.mark.parametrize("status", ["active", "failed"])
async def test_delete_allowed(client: httpx.AsyncClient, pool: asyncpg.Pool, status: str) -> None:
    t = await create_tenant(client, f"d-{status}")
    tid = t["tenant"]["id"]
    v = await force_tenant_status(pool, tid, status)
    r = await client.delete(f"/v1/tenants/{tid}")
    assert r.status_code == 202, r.text
    assert r.json()["tenant"]["status"] == "destroying"
    assert r.json()["tenant"]["version"] == v + 1
    assert r.json()["task"]["type"] == "destroy"


@pytest.mark.parametrize("status", ["provisioning", "updating", "destroying", "destroyed"])
async def test_delete_not_allowed(
    client: httpx.AsyncClient, pool: asyncpg.Pool, status: str
) -> None:
    t = await create_tenant(client, f"dn-{status}")
    await force_tenant_status(pool, t["tenant"]["id"], status)
    assert_error(
        await client.delete(f"/v1/tenants/{t['tenant']['id']}"), 409, "tenant_update_not_allowed"
    )


async def test_delete_with_if_match(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    t = await create_tenant(client, "if-match")
    tid = t["tenant"]["id"]
    v = await force_tenant_status(pool, tid, "active")
    assert_error(
        await client.delete(f"/v1/tenants/{tid}", headers={"If-Match": "abc"}),
        422,
        "validation_error",
    )
    assert_error(
        await client.delete(f"/v1/tenants/{tid}", headers={"If-Match": str(v + 5)}),
        409,
        "tenant_version_conflict",
    )
    r = await client.delete(f"/v1/tenants/{tid}", headers={"If-Match": f'"{v}"'})
    assert r.status_code == 202


# ------------------------------------------------------------------ list + pagination
async def test_tenant_pagination_and_filter(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    ids = [(await create_tenant(client, f"page-{i}"))["tenant"]["id"] for i in range(5)]
    await force_tenant_status(pool, ids[0], "active")

    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params = {"limit": 2} | ({"cursor": cursor} if cursor else {})
        r = await client.get("/v1/tenants", params=params)
        assert r.status_code == 200
        page = r.json()
        seen += [t["id"] for t in page["items"]]
        pages += 1
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert seen == ids  # stable creation order, no gaps, no duplicates
    assert pages == 3

    r = await client.get("/v1/tenants", params={"status": "active"})
    assert [t["id"] for t in r.json()["items"]] == [ids[0]]
    assert_error(
        await client.get("/v1/tenants", params={"cursor": "garbage!"}), 422, "validation_error"
    )
    assert_error(
        await client.get("/v1/tenants", params={"status": "bogus"}), 422, "validation_error"
    )
    assert_error(await client.get("/v1/tenants", params={"limit": 0}), 422, "validation_error")


async def test_task_listing(client: httpx.AsyncClient, pool: asyncpg.Pool) -> None:
    a = await create_tenant(client, "task-a")
    b = await create_tenant(client, "task-b")
    aid = a["tenant"]["id"]
    v = await force_tenant_status(pool, aid, "active")
    await client.patch(f"/v1/tenants/{aid}", json={"version": v, "name": "A2"})

    all_tasks = (await client.get("/v1/tasks")).json()["items"]
    assert len(all_tasks) == 3
    a_tasks = (await client.get(f"/v1/tenants/{aid}/tasks")).json()["items"]
    assert [t["type"] for t in a_tasks] == ["deploy", "update"]
    assert (await client.get("/v1/tasks", params={"tenant_id": aid})).json()["items"] == a_tasks
    upd = (await client.get("/v1/tasks", params={"type": "update"})).json()["items"]
    assert [t["tenant_id"] for t in upd] == [aid]
    acc = (await client.get("/v1/tasks", params={"status": "accepted"})).json()["items"]
    assert {t["tenant_id"] for t in acc} == {aid, b["tenant"]["id"]}

    one = await client.get(f"/v1/tasks/{a_tasks[0]['id']}")
    assert one.status_code == 200
    assert one.json() == a_tasks[0]
    assert UUID(one.json()["id"])
