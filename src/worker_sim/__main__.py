"""Entrypoint: `worker-sim [--fail-rate=1] [--min-delay-ms=..] ...` (or WORKER_* env vars)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Sequence

from worker_sim.config import parse_args
from worker_sim.simulator import Worker


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    cfg = parse_args(argv)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(Worker(cfg).run())


if __name__ == "__main__":
    main()
