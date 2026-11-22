"""Tenant use-cases: each public method is exactly one database transaction.

Inside that transaction we write (1) the tenant change, (2) the new task and
(3) an outbox row describing the task. Either all three commit or none do.
Nothing is sent to Kafka here; the outbox relay does that after commit.
"""

from __future__ import annotations

from typing import NoReturn
from uuid import UUID, uuid4

import asyncpg

from control_plane import repository as repo
from control_plane.domain import (
    DELETABLE_FROM,
    UPDATABLE_FROM,
    TaskNotFound,
    TaskStatus,
    TaskType,
    TenantAlreadyExists,
    TenantNotFound,
    TenantStatus,
    TenantUpdateNotAllowed,
    TenantVersionConflict,
)
from control_plane.messaging.contracts import TaskEvent, TaskRef, TenantRef
from control_plane.pagination import Cursor
from control_plane.repository import Task, Tenant
from control_plane.timeutil import utcnow_ms


class TenantService:
    def __init__(self, pool: asyncpg.Pool, *, tasks_topic: str) -> None:
        self._pool = pool
        self._tasks_topic = tasks_topic

    # ------------------------------------------------------------------- commands
    async def create_tenant(self, *, slug: str, name: str) -> tuple[Tenant, Task]:
        now = utcnow_ms()
        async with self._pool.acquire() as conn, conn.transaction():
            tenant = await repo.insert_tenant_if_slug_free(
                conn, id=uuid4(), slug=slug, name=name, now=now
            )
            if tenant is None:
                raise TenantAlreadyExists(f"a tenant with slug '{slug}' already exists", slug=slug)
            task = await self._start_task(conn, tenant, TaskType.DEPLOY)
        return tenant, task

    async def update_tenant(
        self, tenant_id: UUID, *, version: int, name: str | None
    ) -> tuple[Tenant, Task]:
        async with self._pool.acquire() as conn, conn.transaction():
            tenant = await repo.transition_tenant(
                conn,
                tenant_id=tenant_id,
                allowed_from=UPDATABLE_FROM,
                to=TenantStatus.UPDATING,
                now=utcnow_ms(),
                expected_version=version,
                name=name,
            )
            if tenant is None:
                await self._explain_guard_failure(
                    conn, tenant_id, version, UPDATABLE_FROM, "update"
                )
            task = await self._start_task(conn, tenant, TaskType.UPDATE)
        return tenant, task

    async def delete_tenant(
        self, tenant_id: UUID, *, version: int | None = None
    ) -> tuple[Tenant, Task]:
        async with self._pool.acquire() as conn, conn.transaction():
            tenant = await repo.transition_tenant(
                conn,
                tenant_id=tenant_id,
                allowed_from=DELETABLE_FROM,
                to=TenantStatus.DESTROYING,
                now=utcnow_ms(),
                expected_version=version,
            )
            if tenant is None:
                await self._explain_guard_failure(
                    conn, tenant_id, version, DELETABLE_FROM, "delete"
                )
            task = await self._start_task(conn, tenant, TaskType.DESTROY)
        return tenant, task

    # -------------------------------------------------------------------- queries
    async def get_tenant(self, tenant_id: UUID) -> Tenant:
        async with self._pool.acquire() as conn:
            tenant = await repo.get_tenant(conn, tenant_id)
        if tenant is None:
            raise TenantNotFound(f"tenant {tenant_id} not found", tenant_id=str(tenant_id))
        return tenant

    async def list_tenants(
        self, *, limit: int, after: Cursor | None, status: TenantStatus | None
    ) -> list[Tenant]:
        async with self._pool.acquire() as conn:
            return await repo.list_tenants(conn, limit=limit, after=after, status=status)

    async def get_task(self, task_id: UUID) -> Task:
        async with self._pool.acquire() as conn:
            task = await repo.get_task(conn, task_id)
        if task is None:
            raise TaskNotFound(f"task {task_id} not found", task_id=str(task_id))
        return task

    async def list_tasks(
        self,
        *,
        limit: int,
        after: Cursor | None,
        tenant_id: UUID | None = None,
        status: TaskStatus | None = None,
        type: TaskType | None = None,
    ) -> list[Task]:
        async with self._pool.acquire() as conn:
            if tenant_id is not None and await repo.get_tenant(conn, tenant_id) is None:
                raise TenantNotFound(f"tenant {tenant_id} not found", tenant_id=str(tenant_id))
            return await repo.list_tasks(
                conn, limit=limit, after=after, tenant_id=tenant_id, status=status, type=type
            )

    # -------------------------------------------------------------------- helpers
    async def _start_task(self, conn: asyncpg.Connection, tenant: Tenant, type: TaskType) -> Task:
        """Insert the task and its outbox event in the caller's transaction."""
        task = await repo.insert_task(
            conn, id=uuid4(), tenant_id=tenant.id, type=type, now=tenant.updated_at
        )
        event = TaskEvent(
            event_id=task.id,
            occurred_at=task.created_at,
            task=TaskRef(
                id=task.id,
                type=task.type,
                tenant_id=task.tenant_id,
                status=task.status,
                created_at=task.created_at,
            ),
            tenant=TenantRef(
                id=tenant.id,
                slug=tenant.slug,
                name=tenant.name,
                status=tenant.status,
                version=tenant.version,
            ),
        )
        await repo.insert_outbox(
            conn,
            event_id=task.id,
            topic=self._tasks_topic,
            key=str(tenant.id),  # per-tenant ordering on the topic
            payload=event.model_dump(mode="json"),
        )
        return task

    @staticmethod
    async def _explain_guard_failure(
        conn: asyncpg.Connection,
        tenant_id: UUID,
        version: int | None,
        allowed_from: frozenset[TenantStatus],
        action: str,
    ) -> NoReturn:
        """The compare-and-set matched nothing; work out which guard failed and raise."""
        current = await repo.get_tenant(conn, tenant_id)
        if current is None:
            raise TenantNotFound(f"tenant {tenant_id} not found", tenant_id=str(tenant_id))
        # Version first: a stale client should re-read before we tell it anything else.
        if version is not None and current.version != version:
            raise TenantVersionConflict(
                f"tenant was modified concurrently (expected version {version}, "
                f"current is {current.version})",
                expected_version=version,
                current_version=current.version,
            )
        raise TenantUpdateNotAllowed(
            f"cannot {action} a tenant in status '{current.status}'",
            current_status=current.status.value,
            allowed_statuses=sorted(s.value for s in allowed_from),
        )
