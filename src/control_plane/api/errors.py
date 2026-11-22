"""Every error leaves the API as `{"error": {"code", "message", "details"}}`.

| code                       | HTTP |
|----------------------------|------|
| validation_error           | 422  |
| tenant_not_found           | 404  |
| task_not_found             | 404  |
| not_found (unknown route)  | 404  |
| method_not_allowed         | 405  |
| tenant_already_exists      | 409  |
| tenant_update_not_allowed  | 409  |
| tenant_version_conflict    | 409  |
| internal_error             | 500  |
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from control_plane.domain import DomainError

log = logging.getLogger(__name__)


class ValidationFailed(DomainError):
    code, http_status = "validation_error", 422


def error_response(
    status: int, code: str, message: str, details: dict[str, Any] | None = None
) -> JSONResponse:
    body = {"error": {"code": code, "message": message, "details": details or {}}}
    return JSONResponse(status_code=status, content=jsonable_encoder(body))


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def _domain(_: Request, exc: DomainError) -> JSONResponse:
        return error_response(exc.http_status, exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {"loc": list(e.get("loc", ())), "msg": e.get("msg", ""), "type": e.get("type", "")}
            for e in exc.errors()
        ]
        return error_response(
            422, "validation_error", "request validation failed", {"errors": errors}
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "http_error")
        return error_response(exc.status_code, code, str(exc.detail))

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
        # Log the stack trace server-side; never leak it to the client.
        log.exception("unhandled error", exc_info=exc)
        return error_response(500, "internal_error", "internal server error")
