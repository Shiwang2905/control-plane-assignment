"""Worker simulator planning logic and CLI parsing."""

from __future__ import annotations

import dataclasses
import random
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from control_plane.messaging.contracts import TaskEvent, TaskRef, TenantRef
from worker_sim.config import WorkerConfig, parse_args
from worker_sim.simulator import plan_updates


def event() -> TaskEvent:
    now = datetime.now(UTC)
    tenant_id, task_id = uuid4(), uuid4()
    return TaskEvent(
        event_id=task_id,
        occurred_at=now,
        task=TaskRef(
            id=task_id, type="deploy", tenant_id=tenant_id, status="accepted", created_at=now
        ),
        tenant=TenantRef(id=tenant_id, slug="acme", name="Acme", status="provisioning", version=1),
    )


def cfg(**kw: object) -> WorkerConfig:
    return dataclasses.replace(parse_args([]), **kw)  # type: ignore[arg-type]


def statuses(e: TaskEvent, c: WorkerConfig, seed: int = 0) -> list[str]:
    return [p.update.status for p in plan_updates(e, c, random.Random(seed))]


def test_happy_path() -> None:
    e = event()
    plan = plan_updates(e, cfg(), random.Random(0))
    assert [p.update.status for p in plan] == ["in_progress", "done"]
    assert [p.update.message_id for p in plan] == [f"{e.task.id}:in_progress", f"{e.task.id}:done"]
    assert all(p.update.task_id == e.task.id for p in plan)
    assert all(0.5 <= p.delay_s <= 2.0 for p in plan)


def test_fail_rate_one_always_fails() -> None:
    plan = plan_updates(event(), cfg(fail_rate=1.0), random.Random(0))
    assert [p.update.status for p in plan] == ["in_progress", "failed"]
    assert plan[1].update.error == "simulated deploy failure"


def test_outcome_is_deterministic_per_task() -> None:
    """A redelivered task produces identical reports -> pure duplicates downstream."""
    c = cfg(fail_rate=0.5)
    for _ in range(20):
        e = event()
        assert statuses(e, c, seed=1) == statuses(e, c, seed=2)


def test_fail_rate_is_roughly_respected() -> None:
    c = cfg(fail_rate=0.3)
    failed = sum(statuses(event(), c)[-1] == "failed" for _ in range(2000))
    assert 450 < failed < 750


def test_duplicate_and_reorder_knobs() -> None:
    e = event()
    assert statuses(e, cfg(duplicate_rate=1.0)) == ["in_progress", "in_progress", "done", "done"]
    assert statuses(e, cfg(reorder_rate=1.0)) == ["done", "in_progress"]


def test_zero_delay() -> None:
    plan = plan_updates(event(), cfg(min_delay_ms=0, max_delay_ms=0), random.Random(0))
    assert all(p.delay_s == 0 for p in plan)


def test_cli_args_override_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_FAIL_RATE", "0.2")
    assert parse_args([]).fail_rate == 0.2
    c = parse_args(["--fail-rate=1", "--min-delay-ms=10", "--max-delay-ms=20"])
    assert (c.fail_rate, c.min_delay_ms, c.max_delay_ms) == (1.0, 10, 20)


@pytest.mark.parametrize(
    "argv",
    [
        ["--fail-rate=2"],
        ["--fail-rate=-0.1"],
        ["--min-delay-ms=-1"],
        ["--min-delay-ms=9", "--max-delay-ms=1"],
    ],
)
def test_cli_rejects_bad_values(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        parse_args(argv)
