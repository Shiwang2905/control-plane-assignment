"""Domain model: enums, state machines and domain errors.

This module is pure (no I/O). Every rule about which transitions are legal lives
here, so it can be unit-tested exhaustively and reused by both the HTTP layer and
the message consumer.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Any


class TenantStatus(StrEnum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    UPDATING = "updating"
    DESTROYING = "destroying"
    DESTROYED = "destroyed"
    FAILED = "failed"


class TaskType(StrEnum):
    DEPLOY = "deploy"
    UPDATE = "update"
    DESTROY = "destroy"


class TaskStatus(StrEnum):
    ACCEPTED = "accepted"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self in (TaskStatus.DONE, TaskStatus.FAILED)


SLUG_RE = re.compile(r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$")

# --------------------------------------------------------------------------- task FSM
# Tasks only move forward. Rank encodes "how far along" a status is; done and failed
# share the top rank and are terminal. A forward jump (accepted -> done) is allowed
# because the in_progress message may simply arrive late (order tolerance); the late
# in_progress is then rejected as stale because it would move the task backwards.
_TASK_RANK: dict[TaskStatus, int] = {
    TaskStatus.ACCEPTED: 0,
    TaskStatus.IN_PROGRESS: 1,
    TaskStatus.DONE: 2,
    TaskStatus.FAILED: 2,
}


def task_transition_allowed(current: TaskStatus, new: TaskStatus) -> bool:
    """True if a worker update may move a task from `current` to `new`."""
    if current.is_terminal:
        return False  # terminal is final: no regression, no done<->failed flip
    return _TASK_RANK[new] > _TASK_RANK[current]


# ------------------------------------------------------------------------- tenant FSM
# API-driven transitions (guards).
UPDATABLE_FROM: frozenset[TenantStatus] = frozenset({TenantStatus.ACTIVE})
DELETABLE_FROM: frozenset[TenantStatus] = frozenset({TenantStatus.ACTIVE, TenantStatus.FAILED})

# While a task of a given type is open, the tenant must be in this status.
TENANT_STATUS_FOR_OPEN_TASK: dict[TaskType, TenantStatus] = {
    TaskType.DEPLOY: TenantStatus.PROVISIONING,
    TaskType.UPDATE: TenantStatus.UPDATING,
    TaskType.DESTROY: TenantStatus.DESTROYING,
}

_TENANT_STATUS_ON_DONE: dict[TaskType, TenantStatus] = {
    TaskType.DEPLOY: TenantStatus.ACTIVE,
    TaskType.UPDATE: TenantStatus.ACTIVE,
    TaskType.DESTROY: TenantStatus.DESTROYED,
}


def tenant_status_after_task(task_type: TaskType, outcome: TaskStatus) -> TenantStatus:
    """Tenant status that a *terminal* task outcome leads to."""
    if not outcome.is_terminal:
        raise ValueError(f"{outcome} is not a terminal task status")
    if outcome is TaskStatus.FAILED:
        return TenantStatus.FAILED
    return _TENANT_STATUS_ON_DONE[task_type]


# ----------------------------------------------------------------------------- errors
class DomainError(Exception):
    """Base for errors that map to a structured API response.

    `code` is part of the public contract; clients branch on it.
    """

    code: str = "internal_error"
    http_status: int = 500

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class TenantNotFound(DomainError):
    code, http_status = "tenant_not_found", 404


class TaskNotFound(DomainError):
    code, http_status = "task_not_found", 404


class TenantAlreadyExists(DomainError):
    code, http_status = "tenant_already_exists", 409


class TenantUpdateNotAllowed(DomainError):
    """The tenant's current status does not permit this mutation (PATCH or DELETE)."""

    code, http_status = "tenant_update_not_allowed", 409


class TenantVersionConflict(DomainError):
    """The client's `version` is stale: someone else mutated the tenant first."""

    code, http_status = "tenant_version_conflict", 409
