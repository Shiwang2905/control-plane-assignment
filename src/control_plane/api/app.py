"""FastAPI application factory and process lifecycle.

One process runs three cooperating loops on the same event loop:
  * the HTTP API (uvicorn),
  * the outbox relay   (DB -> Kafka `tenant.tasks`),
  * the update consumer (Kafka `tenant.task-updates` -> DB).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from control_plane.api.errors import error_response, install_error_handlers
from control_plane.api.routes import router
from control_plane.config import Settings
from control_plane.db import create_pool, run_migrations
from control_plane.service import TenantService

log = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = await create_pool(settings.database_url, settings.db_pool_min, settings.db_pool_max)
        await run_migrations(pool)
        app.state.pool = pool
        app.state.service = TenantService(pool, tasks_topic=settings.tasks_topic)

        background: list[asyncio.Task[None]] = []
        # Imported lazily so the API can run (e.g. in tests) without Kafka libraries touching
        # the network.
        if settings.enable_relay:
            from control_plane.messaging.relay import OutboxRelay

            background.append(
                asyncio.create_task(OutboxRelay(pool, settings).run(), name="outbox-relay")
            )
        if settings.enable_consumer:
            from control_plane.messaging.consumer import UpdateConsumer

            background.append(
                asyncio.create_task(UpdateConsumer(pool, settings).run(), name="update-consumer")
            )
        try:
            yield
        finally:
            for t in background:
                t.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            await pool.close()

    app = FastAPI(
        title="Tenant Control Plane",
        version="1.0.0",
        description="Tenants are mutated through asynchronous tasks executed by workers.",
        lifespan=lifespan,
    )
    install_error_handlers(app)
    app.include_router(router)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz(request: Request) -> JSONResponse:
        try:
            async with request.app.state.pool.acquire() as conn:
                await conn.execute("SELECT 1")
        except Exception:
            return error_response(503, "not_ready", "database unavailable")
        return JSONResponse({"status": "ready"})

    return app
