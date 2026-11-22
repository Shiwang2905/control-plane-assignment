"""Worker simulator: consume `tenant.tasks`, fake the work, report progress.

Planning (what to send, in which order, with which delays) is a pure function so it
can be unit-tested; the Kafka loop around it is deliberately small.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from pydantic import ValidationError

from control_plane.backoff import Backoff
from control_plane.messaging.contracts import TaskEvent, TaskUpdate
from worker_sim.config import WorkerConfig

log = logging.getLogger("worker_sim")


@dataclass(frozen=True)
class PlannedUpdate:
    delay_s: float  # wait this long before sending
    update: TaskUpdate


def message_id(task_id: str, status: str) -> str:
    """Deterministic idempotency key: the same report for the same task always has the
    same id, so a re-sent report (worker retry, task event delivered twice) dedupes."""
    return f"{task_id}:{status}"


def plan_updates(event: TaskEvent, cfg: WorkerConfig, rng: random.Random) -> list[PlannedUpdate]:
    task_id = str(event.task.id)
    # Outcome depends only on (seed, task id): a redelivered task gets the same outcome,
    # so its reports are exact duplicates rather than contradicting each other.
    # (Simulation randomness, not cryptography: the stdlib PRNG is fine.)
    outcome_rng = random.Random(f"{cfg.seed}:{task_id}")  # nosec B311
    final = "failed" if outcome_rng.random() < cfg.fail_rate else "done"

    def make(status: str) -> TaskUpdate:
        return TaskUpdate(
            message_id=message_id(task_id, status),
            task_id=event.task.id,
            status=status,  # type: ignore[arg-type]  # validated by the Literal type
            occurred_at=datetime.now(UTC),
            error=f"simulated {event.task.type} failure" if status == "failed" else None,
        )

    updates = [make("in_progress"), make(final)]
    if rng.random() < cfg.reorder_rate:
        updates.reverse()  # terminal first, then a stale in_progress
    planned: list[PlannedUpdate] = []
    for u in updates:
        delay = rng.uniform(cfg.min_delay_ms, cfg.max_delay_ms) / 1000
        planned.append(PlannedUpdate(delay, u))
        if rng.random() < cfg.duplicate_rate:
            planned.append(PlannedUpdate(0.0, u))  # exact same message again
    return planned


class Worker:
    def __init__(self, cfg: WorkerConfig, rng: random.Random | None = None) -> None:
        self.cfg = cfg
        self.rng = rng or random.Random()  # nosec B311

    async def run(self) -> None:
        backoff = Backoff()
        while True:
            try:
                await self._run_once(backoff)
            except asyncio.CancelledError:
                raise
            except Exception:
                delay = backoff.next_delay()
                log.exception("worker crashed; restarting in %.2fs", delay)
                await asyncio.sleep(delay)

    async def _run_once(self, backoff: Backoff) -> None:
        c = self.cfg
        consumer = AIOKafkaConsumer(
            c.tasks_topic,
            bootstrap_servers=c.kafka_bootstrap,
            group_id=c.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=c.kafka_bootstrap, acks="all", enable_idempotence=True
        )
        await consumer.start()
        try:
            await producer.start()
            log.info("worker started: %s", c)
            while True:
                msg = await consumer.getone()
                backoff.reset()
                await self._handle(producer, msg.value)
                # Commit only after every report for this task was acked: if we die
                # mid-task the task event is redelivered and re-simulated.
                await consumer.commit({TopicPartition(msg.topic, msg.partition): msg.offset + 1})
        finally:
            await consumer.stop()
            await producer.stop()

    async def _handle(self, producer: AIOKafkaProducer, raw: bytes | None) -> None:
        try:
            event = TaskEvent.model_validate_json(raw or b"")
        except ValidationError as exc:
            log.error("skipping malformed task event: %s", exc.errors(include_url=False))
            return
        log.info("task %s (%s) for tenant %s", event.task.id, event.task.type, event.tenant.slug)
        for p in plan_updates(event, self.cfg, self.rng):
            await asyncio.sleep(p.delay_s)
            await producer.send_and_wait(
                self.cfg.updates_topic,
                key=str(p.update.task_id).encode(),
                value=p.update.model_dump_json().encode(),
            )
            log.info("  -> %s (%s)", p.update.status, p.update.message_id)
