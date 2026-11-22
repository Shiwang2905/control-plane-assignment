"""Process entrypoint: `control-plane`."""

from __future__ import annotations

import logging

import uvicorn

from control_plane.api.app import create_app
from control_plane.config import Settings


def run() -> None:
    settings = Settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    uvicorn.run(
        create_app(settings),
        host=settings.http_host,
        port=settings.http_port,
        log_level=settings.log_level.lower(),
        # Graceful shutdown: let in-flight requests finish, then cancel background loops.
        timeout_graceful_shutdown=10,
    )


if __name__ == "__main__":
    run()
