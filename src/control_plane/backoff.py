"""Exponential backoff with full jitter, used for every transient failure."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable

log = logging.getLogger(__name__)


class Backoff:
    """delay = random(0, min(max, initial * 2^attempt))  ("full jitter").

    Jitter stops many replicas from retrying in lock-step against a recovering broker/DB.
    """

    def __init__(self, initial: float = 0.2, maximum: float = 10.0) -> None:
        self.initial, self.maximum, self.attempt = initial, maximum, 0

    def next_delay(self) -> float:
        cap = min(self.maximum, self.initial * (2**self.attempt))
        self.attempt += 1
        # Jitter only, not cryptography: the stdlib PRNG is the right tool.
        return random.uniform(0, cap)  # noqa: S311  # nosec B311

    def reset(self) -> None:
        self.attempt = 0

    async def sleep(self) -> None:
        await asyncio.sleep(self.next_delay())


async def retry_forever[T](
    fn: Callable[[], Awaitable[T]],
    *,
    what: str,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    backoff: Backoff | None = None,
) -> T:
    """Call `fn` until it succeeds, sleeping with backoff between failures."""
    b = backoff or Backoff()
    while True:
        try:
            return await fn()
        except retry_on as exc:
            delay = b.next_delay()
            log.warning(
                "%s failed (%s: %s); retrying in %.2fs", what, type(exc).__name__, exc, delay
            )
            await asyncio.sleep(delay)
