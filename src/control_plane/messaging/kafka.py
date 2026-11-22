"""Thin Kafka adapters. Everything Kafka-specific lives here, behind `Publisher`,
so the relay/consumer logic can be tested against a fake without a broker."""

from __future__ import annotations

from typing import Protocol

from aiokafka import AIOKafkaProducer

Headers = list[tuple[str, bytes]]


class Publisher(Protocol):
    async def publish(self, topic: str, *, key: str, value: bytes, headers: Headers) -> None:
        """Return only once the broker has durably acknowledged the record."""


class KafkaPublisher:
    def __init__(self, bootstrap: str, client_id: str) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            client_id=client_id,
            acks="all",  # wait for all in-sync replicas
            enable_idempotence=True,  # broker drops producer-retry duplicates
            linger_ms=5,
            request_timeout_ms=10_000,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, topic: str, *, key: str, value: bytes, headers: Headers) -> None:
        await self._producer.send_and_wait(topic, value=value, key=key.encode(), headers=headers)
