"""Transactional outbox relay: DB `outbox` table -> Kafka `tenant.tasks`.

Why an outbox instead of "commit, then publish" in the request handler:
  * publish-before-commit can announce a task that then rolls back;
  * commit-then-publish loses the event if the process dies between the two steps.
With the outbox the event is part of the same commit as the state change, and this
relay retries publishing until the broker acknowledges it. The result is
"published at least once, and only after commit".

The trade-off is at-least-once: if we crash after Kafka acks but before we mark the
row published, the event is sent again. Consumers dedupe on `event_id` (= task id).
"""

from __future__ import annotations

import asyncio
import json
import logging

import asyncpg

from control_plane.backoff import Backoff, retry_forever
from control_plane.config import Settings
from control_plane.messaging.kafka import KafkaPublisher, Publisher

log = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self, pool: asyncpg.Pool, settings: Settings, publisher: Publisher | None = None
    ) -> None:
        self._pool = pool
        self._settings = settings
        self._publisher = publisher

    async def publish_batch(self) -> int:
        """Publish up to `batch_size` pending events. Returns how many were published.

        FOR UPDATE SKIP LOCKED lets several replicas run relays concurrently: each one
        claims a disjoint set of rows instead of blocking on (or double-sending) the
        others'. The row lock is held until we mark the rows published and commit.
        Rows from transactions that have not committed yet are simply invisible here,
        which is exactly the "never before commit" guarantee.
        """
        if self._publisher is None:
            raise RuntimeError("publisher not started; call run() or pass a publisher")
        async with self._pool.acquire() as conn, conn.transaction():
            rows = await conn.fetch(
                """
                SELECT id, event_id, topic, key, payload
                  FROM outbox
                 WHERE published_at IS NULL
                 ORDER BY id
                 LIMIT $1
                   FOR UPDATE SKIP LOCKED
                """,
                self._settings.outbox_batch_size,
            )
            for r in rows:
                # Sequential + awaited: preserves per-key order. If one publish fails the
                # whole batch rolls back and is retried (already-sent ones get re-sent).
                await self._publisher.publish(
                    r["topic"],
                    key=r["key"],
                    value=json.dumps(r["payload"], separators=(",", ":")).encode(),
                    headers=[
                        ("event_id", str(r["event_id"]).encode()),
                        ("event_type", str(r["payload"].get("event_type", "")).encode()),
                    ],
                )
            if rows:
                await conn.execute(
                    "UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])",
                    [r["id"] for r in rows],
                )
        if rows:
            log.info("outbox relay published %d event(s)", len(rows))
        return len(rows)

    async def run(self) -> None:
        """Forever: drain the outbox, sleep briefly when empty, back off on errors."""
        owned: KafkaPublisher | None = None
        if self._publisher is None:
            owned = KafkaPublisher(self._settings.kafka_bootstrap, client_id="control-plane-relay")
            await retry_forever(owned.start, what="start kafka producer")
            self._publisher = owned
        backoff = Backoff(self._settings.backoff_initial_s, self._settings.backoff_max_s)
        try:
            while True:
                try:
                    n = await self.publish_batch()
                    backoff.reset()
                    if n < self._settings.outbox_batch_size:
                        await asyncio.sleep(self._settings.outbox_poll_interval_s)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    delay = backoff.next_delay()
                    log.exception("outbox relay failed; retrying in %.2fs", delay)
                    await asyncio.sleep(delay)
        finally:
            if owned is not None:
                await owned.stop()
