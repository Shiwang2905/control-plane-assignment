"""Postgres connection pool and a tiny, concurrency-safe migration runner."""

from __future__ import annotations

import json
import logging
from importlib import resources

import asyncpg

from control_plane.backoff import retry_forever

log = logging.getLogger(__name__)

# Arbitrary constant: all replicas take the same advisory lock, so only one of them
# applies migrations at a time even if several boot simultaneously.
_MIGRATION_LOCK_ID = 7_202_611

# Errors worth retrying: network blips, Postgres starting up / failing over,
# serialization/deadlock aborts. Anything else (constraint violations, bad SQL) is a bug
# or a business rule and retrying it would never help.
TRANSIENT_DB_ERRORS: tuple[type[BaseException], ...] = (
    OSError,
    TimeoutError,
    asyncpg.PostgresConnectionError,
    asyncpg.CannotConnectNowError,
    asyncpg.InterfaceError,
    asyncpg.SerializationError,
    asyncpg.DeadlockDetectedError,
)


async def _init_connection(conn: asyncpg.Connection) -> None:
    # Let us pass/receive Python dicts for jsonb columns.
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(dsn: str, min_size: int = 2, max_size: int = 10) -> asyncpg.Pool:
    """Create the pool, retrying with backoff while Postgres is still starting."""

    async def connect() -> asyncpg.Pool:
        return await asyncpg.create_pool(
            dsn, min_size=min_size, max_size=max_size, init=_init_connection
        )

    return await retry_forever(connect, what="connect to postgres", retry_on=TRANSIENT_DB_ERRORS)


async def run_migrations(pool: asyncpg.Pool) -> None:
    files = sorted(
        (
            f
            for f in resources.files("control_plane.migrations").iterdir()
            if f.name.endswith(".sql")
        ),
        key=lambda f: f.name,
    )
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock($1)", _MIGRATION_LOCK_ID)
        try:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
            applied = {r["name"] for r in await conn.fetch("SELECT name FROM schema_migrations")}
            for f in files:
                if f.name in applied:
                    continue
                log.info("applying migration %s", f.name)
                async with conn.transaction():
                    await conn.execute(f.read_text())
                    await conn.execute("INSERT INTO schema_migrations (name) VALUES ($1)", f.name)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_ID)
