"""Worker simulator settings: CLI flags, each defaulting to a WORKER_* env var."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerConfig:
    kafka_bootstrap: str
    tasks_topic: str
    updates_topic: str
    group_id: str
    fail_rate: float
    min_delay_ms: int
    max_delay_ms: int
    duplicate_rate: float
    reorder_rate: float
    seed: str


def _rate(value: str) -> float:
    f = float(value)
    if not 0.0 <= f <= 1.0:
        raise argparse.ArgumentTypeError(f"must be between 0 and 1, got {value}")
    return f


def _non_negative_int(value: str) -> int:
    i = int(value)
    if i < 0:
        raise argparse.ArgumentTypeError(f"must be >= 0, got {value}")
    return i


def parse_args(argv: Sequence[str] | None = None) -> WorkerConfig:
    env = os.environ.get
    p = argparse.ArgumentParser(
        prog="worker-sim",
        description="Consumes provisioning tasks and reports fake progress to the control plane.",
    )
    p.add_argument("--kafka-bootstrap", default=env("WORKER_KAFKA_BOOTSTRAP", "localhost:19092"))
    p.add_argument("--tasks-topic", default=env("WORKER_TASKS_TOPIC", "tenant.tasks"))
    p.add_argument("--updates-topic", default=env("WORKER_UPDATES_TOPIC", "tenant.task-updates"))
    p.add_argument("--group-id", default=env("WORKER_GROUP_ID", "worker-sim"))
    p.add_argument(
        "--fail-rate",
        type=_rate,
        default=env("WORKER_FAIL_RATE", "0"),
        help="probability a task ends `failed` (0..1)",
    )
    p.add_argument(
        "--min-delay-ms",
        type=_non_negative_int,
        default=env("WORKER_MIN_DELAY_MS", "500"),
        help="minimum simulated delay before each update",
    )
    p.add_argument(
        "--max-delay-ms",
        type=_non_negative_int,
        default=env("WORKER_MAX_DELAY_MS", "2000"),
        help="maximum simulated delay before each update",
    )
    p.add_argument(
        "--duplicate-rate",
        type=_rate,
        default=env("WORKER_DUPLICATE_RATE", "0"),
        help="probability each update is published twice (tests idempotency)",
    )
    p.add_argument(
        "--reorder-rate",
        type=_rate,
        default=env("WORKER_REORDER_RATE", "0"),
        help="probability the terminal update is sent BEFORE in_progress (tests ordering)",
    )
    p.add_argument(
        "--seed",
        default=env("WORKER_SEED", "worker-sim"),
        help="makes outcomes deterministic per task id",
    )
    a = p.parse_args(argv)
    if a.min_delay_ms > a.max_delay_ms:
        p.error("--min-delay-ms must be <= --max-delay-ms")
    return WorkerConfig(
        kafka_bootstrap=a.kafka_bootstrap,
        tasks_topic=a.tasks_topic,
        updates_topic=a.updates_topic,
        group_id=a.group_id,
        fail_rate=a.fail_rate,
        min_delay_ms=a.min_delay_ms,
        max_delay_ms=a.max_delay_ms,
        duplicate_rate=a.duplicate_rate,
        reorder_rate=a.reorder_rate,
        seed=a.seed,
    )
