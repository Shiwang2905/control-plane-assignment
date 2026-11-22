"""Shared fixtures. Integration tests run against a real Postgres (never mocks):
locking and unique-constraint behaviour is exactly what we are testing."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from urllib.parse import urlparse, urlunparse
from uuid import UUID

import asyncpg
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from control_plane.api.app import create_app
from control_plane.config import Settings

TEST_DB_URL = os.environ.get(
    "CP_TEST_DATABASE_URL", "postgresql://postgres@localhost:5432/control_plane_test"
)


async def _ensure_database(url: str) -> None:
    parsed = urlparse(url)
    db_name = parsed.path.lstrip("/")
    admin = await asyncpg.connect(urlunparse(parsed._replace(path="/postgres")))
    try:
        if not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", db_name):
            await admin.execute(f'CREATE DATABASE "{db_name}"')
    finally:
        await admin.close()


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings(
        database_url=TEST_DB_URL,
        db_pool_max=30,
        enable_relay=False,  # tests drive relay/consumer explicitly
        enable_consumer=False,
        backoff_initial_s=0.01,
        backoff_max_s=0.05,
    )


@pytest_asyncio.fixture(scope="session")
async def app(settings: Settings) -> AsyncIterator[FastAPI]:
    await _ensure_database(settings.database_url)
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        yield application


@pytest.fixture
def pool(app: FastAPI) -> asyncpg.Pool:
    p: asyncpg.Pool = app.state.pool
    return p


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ------------------------------------------------------------------------- helpers
async def create_tenant(client: httpx.AsyncClient, slug: str, name: str = "Acme") -> dict:  # type: ignore[type-arg]
    r = await client.post("/v1/tenants", json={"slug": slug, "name": name})
    assert r.status_code == 201, r.text
    return r.json()  # type: ignore[no-any-return]


async def force_tenant_status(pool: asyncpg.Pool, tenant_id: str | UUID, status: str) -> int:
    """Test shortcut: close open tasks and put the tenant in `status`. Returns new version."""
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "UPDATE tasks SET status='done'"
            " WHERE tenant_id=$1 AND status IN ('accepted','in_progress')",
            UUID(str(tenant_id)),
        )
        v: int = await conn.fetchval(
            "UPDATE tenants SET status=$2, version=version+1 WHERE id=$1 RETURNING version",
            UUID(str(tenant_id)),
            status,
        )
    return v
