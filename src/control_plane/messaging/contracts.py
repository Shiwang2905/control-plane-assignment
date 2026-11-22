"""Wire contracts for both directions on the broker.

Outbound  (control plane -> worker), topic `tenant.tasks`, key = tenant_id:
    TaskEvent: the task itself, persisted then published. `event_id` == task id.

Inbound   (worker -> control plane), topic `tenant.task-updates`, key = task_id:
    TaskUpdate: one progress report. `message_id` is the idempotency key.

Both carry `schema_version` so the format can evolve without breaking old consumers.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from control_plane.domain import TaskStatus, TaskType, TenantStatus


class _Contract(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")  # tolerate additive changes


class TaskRef(_Contract):
    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus
    created_at: datetime


class TenantRef(_Contract):
    id: UUID
    slug: str
    name: str
    status: TenantStatus
    version: int


class TaskEvent(_Contract):
    schema_version: Literal[1] = 1
    event_id: UUID  # == task.id
    event_type: Literal["task.accepted"] = "task.accepted"
    occurred_at: datetime
    task: TaskRef
    tenant: TenantRef  # snapshot so the worker knows *what* to provision


# Statuses a worker is allowed to report. `accepted` is set only by the control plane.
ReportedStatus = Literal["in_progress", "done", "failed"]


class TaskUpdate(_Contract):
    schema_version: Literal[1] = 1
    message_id: str = Field(min_length=1, max_length=200)  # idempotency key
    task_id: UUID
    status: ReportedStatus
    occurred_at: datetime
    error: str | None = Field(default=None, max_length=2000)
