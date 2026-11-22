"""In-memory test doubles for the broker side."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

from control_plane.messaging.kafka import Headers


@dataclass
class Sent:
    topic: str
    key: str
    value: bytes
    headers: Headers

    @property
    def json(self) -> Any:
        return json.loads(self.value)

    def header(self, name: str) -> str:
        return dict(self.headers)[name].decode()


@dataclass
class FakePublisher:
    """Records publishes. `fail_next` makes the next N publishes raise (broker outage)."""

    sent: list[Sent] = field(default_factory=list)
    fail_next: int = 0
    delay_s: float = 0.0

    async def publish(self, topic: str, *, key: str, value: bytes, headers: Headers) -> None:
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        if self.fail_next > 0:
            self.fail_next -= 1
            raise ConnectionError("broker unavailable (simulated)")
        self.sent.append(Sent(topic, key, value, headers))
