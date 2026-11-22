"""Data access: plain SQL, one function per query.

Every function takes an open connection so the *caller* decides the transaction
boundary. Nothing here commits on its own.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from control_plane.domain import TaskStatus, TaskType, TenantStatus
from control_plane.pagination import Cursor


def _required(row: asyncpg.Record | None) -> asyncpg.Record:
    """For statements that must return a row (INSERT/UPDATE ... RETURNING on a locked row)."""
    if row is None:
        raise RuntimeError("expected a row, got none")
    return row


@dataclass(frozen=True)
class Tenant:
    id: UUID
    slug: str
    name: str
    status: TenantStatus
    version: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, r: asyncpg.Record) -> Tenant:
        return cls(
            id=r["id"],
            slug=r["slug"],
            name=r["name"],
            status=TenantStatus(r["status"]),
            version=r["version"],
            created_at=r["created_at"],
            updated_at=r["updated_at"],
        )


@dataclass(frozen=True)
class Task:
    id: UUID
    tenant_id: UUID
    type: TaskType
    status: TaskStatus
    error: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, r: asyncpg.Record) -> Task:
        return cls(
            id=r["id"],
            tenant_id=r["tenant_id"],
            type=TaskType(r["type"]),
            status=TaskStatus(r["status"]),
            error=r["error"],
            created_at=r["created_at"],
            updated_at=r["updated_at"],
        )


# ------------------------------------------------------------------------ tenants
_SELECT_TENANT = (
    "SELECT id, slug, name, status, version, created_at, updated_at FROM tenants WHERE id = $1"
)
_SELECT_TENANT_FOR_UPDATE = _SELECT_TENANT + " FOR UPDATE"


async def insert_tenant_if_slug_free(
    conn: asyncpg.Connection, *, id: UUID, slug: str, name: str, now: datetime
) -> Tenant | None:
    """INSERT ... ON CONFLICT DO NOTHING: returns None if the slug is already taken.

    Under concurrency Postgres makes the second inserter wait on the first one's
    uncommitted index entry; once the first commits, the second sees the conflict and
    inserts nothing. Exactly one winner, no exception handling, no app-level lock.
    """
    row = await conn.fetchrow(
        """
        INSERT INTO tenants (id, slug, name, status, version, created_at, updated_at)
        VALUES ($1, $2, $3, $4, 1, $5, $5)
        ON CONFLICT (slug) DO NOTHING
        RETURNING id, slug, name, status, version, created_at, updated_at
        """,
        id,
        slug,
        name,
        TenantStatus.PROVISIONING.value,
        now,
    )
    return Tenant.from_row(row) if row else None


async def get_tenant(
    conn: asyncpg.Connection, tenant_id: UUID, *, for_update: bool = False
) -> Tenant | None:
    sql = _SELECT_TENANT_FOR_UPDATE if for_update else _SELECT_TENANT
    row = await conn.fetchrow(sql, tenant_id)
    return Tenant.from_row(row) if row else None


async def transition_tenant(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    allowed_from: frozenset[TenantStatus],
    to: TenantStatus,
    now: datetime,
    expected_version: int | None = None,
    name: str | None = None,
) -> Tenant | None:
    """Compare-and-set on (status, version). Returns None if the guard did not match.

    The WHERE clause *is* the optimistic lock. Two concurrent PATCHes with version=3:
    the first takes the row lock and sets version=4. The second blocks on that row lock;
    when the first commits, Postgres (READ COMMITTED) re-evaluates the WHERE against the
    new row, sees version=4 != 3, and updates 0 rows. No lost update is possible.
    """
    row = await conn.fetchrow(
        """
        UPDATE tenants
           SET status = $3,
               name = COALESCE($5, name),
               version = version + 1,
               updated_at = $4
         WHERE id = $1
           AND status = ANY($2::text[])
           AND ($6::int IS NULL OR version = $6)
        RETURNING id, slug, name, status, version, created_at, updated_at
        """,
        tenant_id,
        [s.value for s in allowed_from],
        to.value,
        now,
        name,
        expected_version,
    )
    return Tenant.from_row(row) if row else None


async def list_tenants(
    conn: asyncpg.Connection, *, limit: int, after: Cursor | None, status: TenantStatus | None
) -> list[Tenant]:
    rows = await conn.fetch(
        """
        SELECT id, slug, name, status, version, created_at, updated_at FROM tenants
         WHERE ($1::text IS NULL OR status = $1)
           AND ($2::timestamptz IS NULL OR (created_at, id) > ($2, $3::uuid))
         ORDER BY created_at, id
         LIMIT $4
        """,
        status.value if status else None,
        after.created_at if after else None,
        after.id if after else None,
        limit,
    )
    return [Tenant.from_row(r) for r in rows]


# -------------------------------------------------------------------------- tasks
_SELECT_TASK = (
    "SELECT id, tenant_id, type, status, error, created_at, updated_at FROM tasks WHERE id = $1"
)
_SELECT_TASK_FOR_UPDATE = _SELECT_TASK + " FOR UPDATE"


async def insert_task(
    conn: asyncpg.Connection, *, id: UUID, tenant_id: UUID, type: TaskType, now: datetime
) -> Task:
    row = await conn.fetchrow(
        """
        INSERT INTO tasks (id, tenant_id, type, status, created_at, updated_at)
        VALUES ($1, $2, $3, 'accepted', $4, $4)
        RETURNING id, tenant_id, type, status, error, created_at, updated_at
        """,
        id,
        tenant_id,
        type.value,
        now,
    )
    return Task.from_row(_required(row))


async def get_task(
    conn: asyncpg.Connection, task_id: UUID, *, for_update: bool = False
) -> Task | None:
    sql = _SELECT_TASK_FOR_UPDATE if for_update else _SELECT_TASK
    row = await conn.fetchrow(sql, task_id)
    return Task.from_row(row) if row else None


async def set_task_status(
    conn: asyncpg.Connection, *, task_id: UUID, status: TaskStatus, error: str | None, now: datetime
) -> Task:
    row = await conn.fetchrow(
        """
        UPDATE tasks SET status = $2, error = COALESCE($3, error), updated_at = $4
         WHERE id = $1
        RETURNING id, tenant_id, type, status, error, created_at, updated_at
        """,
        task_id,
        status.value,
        error,
        now,
    )
    return Task.from_row(_required(row))


async def list_tasks(
    conn: asyncpg.Connection,
    *,
    limit: int,
    after: Cursor | None,
    tenant_id: UUID | None,
    status: TaskStatus | None,
    type: TaskType | None,
) -> list[Task]:
    rows = await conn.fetch(
        """
        SELECT id, tenant_id, type, status, error, created_at, updated_at FROM tasks
         WHERE ($1::uuid IS NULL OR tenant_id = $1)
           AND ($2::text IS NULL OR status = $2)
           AND ($3::text IS NULL OR type = $3)
           AND ($4::timestamptz IS NULL OR (created_at, id) > ($4, $5::uuid))
         ORDER BY created_at, id
         LIMIT $6
        """,
        tenant_id,
        status.value if status else None,
        type.value if type else None,
        after.created_at if after else None,
        after.id if after else None,
        limit,
    )
    return [Task.from_row(r) for r in rows]


# ------------------------------------------------------------------------- outbox
async def insert_outbox(
    conn: asyncpg.Connection, *, event_id: UUID, topic: str, key: str, payload: dict[str, Any]
) -> None:
    await conn.execute(
        "INSERT INTO outbox (event_id, topic, key, payload) VALUES ($1, $2, $3, $4)",
        event_id,
        topic,
        key,
        payload,
    )


# ------------------------------------------------------------- inbound idempotency
async def claim_message(conn: asyncpg.Connection, *, message_id: str, task_id: UUID | None) -> bool:
    """Record that we are handling `message_id`. False means we've already handled it.

    Runs inside the same transaction as the state change it guards, so "marked as
    processed" and "effect applied" commit or roll back together.
    """
    row = await conn.fetchrow(
        """
        INSERT INTO processed_messages (message_id, task_id, outcome)
        VALUES ($1, $2, 'pending')
        ON CONFLICT (message_id) DO NOTHING
        RETURNING message_id
        """,
        message_id,
        task_id,
    )
    return row is not None


async def set_message_outcome(conn: asyncpg.Connection, *, message_id: str, outcome: str) -> None:
    await conn.execute(
        "UPDATE processed_messages SET outcome = $2 WHERE message_id = $1", message_id, outcome
    )
