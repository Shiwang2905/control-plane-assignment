"""Apply one worker progress update to the database: idempotent and order-tolerant.

All of the following happen in ONE transaction:
  1. lock the tenant, then the task   (same lock order as the API: no deadlocks)
  2. claim the message_id in processed_messages   (duplicate -> no-op)
  3. check the task state machine                 (stale/out-of-order -> no-op)
  4. update the task; on a terminal outcome, move the tenant (version + 1)
Because the dedup record and the effect commit together, a crash at any point either
leaves both or neither: redelivery after a crash re-applies cleanly.
"""

from __future__ import annotations

import json
import logging
from enum import StrEnum

import asyncpg
from pydantic import ValidationError

from control_plane import repository as repo
from control_plane.domain import (
    TENANT_STATUS_FOR_OPEN_TASK,
    TaskStatus,
    task_transition_allowed,
    tenant_status_after_task,
)
from control_plane.messaging.contracts import TaskUpdate
from control_plane.timeutil import utcnow_ms

log = logging.getLogger(__name__)


class Outcome(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"  # same message_id seen before
    STALE = "stale"  # valid message, but would move the task backwards / out of terminal


class PoisonMessage(Exception):
    """The message can never be processed (malformed, unknown task). Retrying is useless."""


def parse_update(raw: bytes | None) -> TaskUpdate:
    if raw is None:
        raise PoisonMessage("empty message")
    try:
        return TaskUpdate.model_validate(json.loads(raw))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PoisonMessage(f"not valid JSON: {exc}") from exc
    except ValidationError as exc:
        raise PoisonMessage(f"schema violation: {exc.errors(include_url=False)}") from exc


class UpdateHandler:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def handle(self, raw: bytes | None) -> Outcome:
        update = parse_update(raw)
        async with self._pool.acquire() as conn, conn.transaction():
            outcome = await self._apply(conn, update)
        log.info(
            "update %s for task %s (%s): %s",
            update.message_id,
            update.task_id,
            update.status,
            outcome,
        )
        return outcome

    async def _apply(self, conn: asyncpg.Connection, update: TaskUpdate) -> Outcome:
        # Find the tenant without locking, then lock tenant -> task (API lock order).
        peek = await repo.get_task(conn, update.task_id)
        if peek is None:
            raise PoisonMessage(f"unknown task {update.task_id}")
        tenant = await repo.get_tenant(conn, peek.tenant_id, for_update=True)
        task = await repo.get_task(conn, update.task_id, for_update=True)
        if tenant is None or task is None:  # impossible: FK + we never hard-delete
            raise RuntimeError(f"task {update.task_id} or its tenant vanished")

        if not await repo.claim_message(conn, message_id=update.message_id, task_id=task.id):
            return Outcome.DUPLICATE

        new_status = TaskStatus(update.status)
        if not task_transition_allowed(task.status, new_status):
            await repo.set_message_outcome(
                conn, message_id=update.message_id, outcome=Outcome.STALE
            )
            return Outcome.STALE

        now = utcnow_ms()
        await repo.set_task_status(
            conn, task_id=task.id, status=new_status, error=update.error, now=now
        )

        if new_status.is_terminal:
            expected = TENANT_STATUS_FOR_OPEN_TASK[task.type]
            target = tenant_status_after_task(task.type, new_status)
            moved = await repo.transition_tenant(
                conn, tenant_id=tenant.id, allowed_from=frozenset({expected}), to=target, now=now
            )
            if moved is None:  # should be impossible given the API guards; never crash on it
                log.error(
                    "tenant %s in status %s, expected %s while finishing %s task %s",
                    tenant.id,
                    tenant.status,
                    expected,
                    task.type,
                    task.id,
                )

        await repo.set_message_outcome(conn, message_id=update.message_id, outcome=Outcome.APPLIED)
        return Outcome.APPLIED
