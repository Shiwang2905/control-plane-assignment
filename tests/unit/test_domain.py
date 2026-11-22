"""Exhaustive tests for the pure state-machine rules."""

import itertools

import pytest

from control_plane.domain import (
    DELETABLE_FROM,
    SLUG_RE,
    UPDATABLE_FROM,
    TaskStatus,
    TaskType,
    TenantStatus,
    task_transition_allowed,
    tenant_status_after_task,
)

T = TaskStatus

# Every (current, new) pair that IS allowed. Everything else must be refused.
ALLOWED_TASK_TRANSITIONS = {
    (T.ACCEPTED, T.IN_PROGRESS),
    (T.ACCEPTED, T.DONE),  # in_progress arrived late / was lost
    (T.ACCEPTED, T.FAILED),
    (T.IN_PROGRESS, T.DONE),
    (T.IN_PROGRESS, T.FAILED),
}


@pytest.mark.parametrize(("current", "new"), list(itertools.product(TaskStatus, TaskStatus)))
def test_task_transition_matrix(current: TaskStatus, new: TaskStatus) -> None:
    assert task_transition_allowed(current, new) is ((current, new) in ALLOWED_TASK_TRANSITIONS)


@pytest.mark.parametrize("terminal", [T.DONE, T.FAILED])
def test_terminal_tasks_never_move(terminal: TaskStatus) -> None:
    assert not any(task_transition_allowed(terminal, s) for s in TaskStatus)


@pytest.mark.parametrize(
    ("task_type", "outcome", "expected"),
    [
        (TaskType.DEPLOY, T.DONE, TenantStatus.ACTIVE),
        (TaskType.UPDATE, T.DONE, TenantStatus.ACTIVE),
        (TaskType.DESTROY, T.DONE, TenantStatus.DESTROYED),
        (TaskType.DEPLOY, T.FAILED, TenantStatus.FAILED),
        (TaskType.UPDATE, T.FAILED, TenantStatus.FAILED),
        (TaskType.DESTROY, T.FAILED, TenantStatus.FAILED),
    ],
)
def test_tenant_status_after_terminal_task(
    task_type: TaskType, outcome: TaskStatus, expected: TenantStatus
) -> None:
    assert tenant_status_after_task(task_type, outcome) is expected


@pytest.mark.parametrize("non_terminal", [T.ACCEPTED, T.IN_PROGRESS])
def test_non_terminal_outcome_does_not_move_tenant(non_terminal: TaskStatus) -> None:
    with pytest.raises(ValueError, match="not a terminal"):
        tenant_status_after_task(TaskType.DEPLOY, non_terminal)


def test_api_guards() -> None:
    assert {TenantStatus.ACTIVE} == UPDATABLE_FROM
    assert {TenantStatus.ACTIVE, TenantStatus.FAILED} == DELETABLE_FROM


@pytest.mark.parametrize(
    "slug", ["abc", "a-b", "tenant-1", "a" * 28, "x0" + "-" * 25 + "z", "acme-prod-eu1"]
)
def test_valid_slugs(slug: str) -> None:
    assert SLUG_RE.fullmatch(slug)


@pytest.mark.parametrize(
    "slug",
    ["ab", "a" * 29, "1abc", "-abc", "abc-", "Abc", "ab_c", "ab.c", "", "ab c", "abc\n"],
)
def test_invalid_slugs(slug: str) -> None:
    assert not SLUG_RE.fullmatch(slug)
