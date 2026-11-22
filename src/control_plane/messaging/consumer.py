"""Kafka consumer for `tenant.task-updates`.

Delivery semantics: at-least-once. We commit a record's offset only AFTER its DB
transaction committed (or after it was parked on the DLQ). A crash in between means
redelivery, which the idempotent handler turns into a no-op.

Failure policy, per record:
  * transient DB error (connection lost, failover, deadlock) -> retry forever with
    backoff. We must not skip the record, and blocking the partition is correct:
    the next record would hit the same broken DB anyway.
  * PoisonMessage (bad JSON/schema, unknown task) -> straight to the DLQ.
  * any other exception (a bug) -> retry a few times, then DLQ, so one bad record
    can never wedge the consumer forever.
  * broker errors (fetch/commit) -> the whole consumer is rebuilt with backoff.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import asyncpg
from aiokafka import AIOKafkaConsumer, TopicPartition

from control_plane.backoff import Backoff, retry_forever
from control_plane.config import Settings
from control_plane.db import TRANSIENT_DB_ERRORS
from control_plane.messaging.handler import Outcome, PoisonMessage, UpdateHandler
from control_plane.messaging.kafka import KafkaPublisher, Publisher

log = logging.getLogger(__name__)

MAX_ATTEMPTS_FOR_UNEXPECTED_ERRORS = 5


@dataclass(frozen=True)
class Record:
    """Broker-agnostic view of a consumed message."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


class RecordProcessor:
    """Decides what happens to one record. No Kafka client inside -> unit-testable."""

    def __init__(self, handler: UpdateHandler, dlq: Publisher, settings: Settings) -> None:
        self._handler = handler
        self._dlq = dlq
        self._settings = settings

    def _backoff(self) -> Backoff:
        return Backoff(self._settings.backoff_initial_s, self._settings.backoff_max_s)

    async def process(self, record: Record) -> Outcome | None:
        """Returns the outcome, or None if the record was dead-lettered."""
        backoff = self._backoff()
        unexpected_failures = 0
        while True:
            try:
                return await self._handler.handle(record.value)
            except PoisonMessage as exc:
                await self._dead_letter(record, f"poison: {exc}")
                return None
            except TRANSIENT_DB_ERRORS as exc:
                delay = backoff.next_delay()
                log.warning("transient DB error (%s); retrying record in %.2fs", exc, delay)
                await asyncio.sleep(delay)
            except Exception as exc:
                unexpected_failures += 1
                log.exception("unexpected error processing %s@%d", record.topic, record.offset)
                if unexpected_failures >= MAX_ATTEMPTS_FOR_UNEXPECTED_ERRORS:
                    await self._dead_letter(record, f"unexpected: {type(exc).__name__}: {exc}")
                    return None
                await backoff.sleep()

    async def _dead_letter(self, record: Record, reason: str) -> None:
        log.error(
            "dead-lettering %s[%d]@%d: %s", record.topic, record.partition, record.offset, reason
        )
        key = record.key.decode(errors="replace") if record.key else ""

        async def send() -> None:
            await self._dlq.publish(
                self._settings.dlq_topic,
                key=key,
                value=record.value or b"",
                headers=[
                    ("dlq_reason", reason[:1000].encode()),
                    ("source_topic", record.topic.encode()),
                    ("source_partition", str(record.partition).encode()),
                    ("source_offset", str(record.offset).encode()),
                ],
            )

        # The record is only acknowledged once it is safely parked on the DLQ.
        await retry_forever(send, what="publish to DLQ", backoff=self._backoff())


class UpdateConsumer:
    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings

    async def run(self) -> None:
        backoff = Backoff(self._settings.backoff_initial_s, self._settings.backoff_max_s)
        while True:
            try:
                await self._run_once(backoff)
            except asyncio.CancelledError:
                raise
            except Exception:
                delay = backoff.next_delay()
                log.exception("update consumer crashed; restarting in %.2fs", delay)
                await asyncio.sleep(delay)

    async def _run_once(self, backoff: Backoff) -> None:
        s = self._settings
        consumer = AIOKafkaConsumer(
            s.updates_topic,
            bootstrap_servers=s.kafka_bootstrap,
            group_id=s.consumer_group,
            client_id="control-plane-consumer",
            enable_auto_commit=False,  # we commit manually, after the DB commit
            auto_offset_reset="earliest",
        )
        dlq = KafkaPublisher(s.kafka_bootstrap, client_id="control-plane-dlq")
        await consumer.start()
        try:
            await dlq.start()
            processor = RecordProcessor(UpdateHandler(self._pool), dlq, s)
            log.info("update consumer started on %s", s.updates_topic)
            while True:
                msg = await consumer.getone()
                backoff.reset()
                await processor.process(
                    Record(msg.topic, msg.partition, msg.offset, msg.key, msg.value)
                )
                await consumer.commit({TopicPartition(msg.topic, msg.partition): msg.offset + 1})
        finally:
            await consumer.stop()
            await dlq.stop()
