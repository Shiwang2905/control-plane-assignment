"""REST routes. Thin: validate input, call the service, shape the response."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Annotated, Any, Protocol
from uuid import UUID

from fastapi import APIRouter, Header, Query, Request, Response, status

from control_plane.api.errors import ValidationFailed
from control_plane.api.schemas import (
    ErrorResponse,
    TaskOut,
    TaskPage,
    TenantCreate,
    TenantOperation,
    TenantOut,
    TenantPage,
    TenantPatch,
)
from control_plane.domain import TaskStatus, TaskType, TenantStatus
from control_plane.pagination import Cursor, InvalidCursor
from control_plane.service import TenantService

router = APIRouter(prefix="/v1")

Limit = Annotated[int, Query(ge=1, le=200, description="page size")]
CursorQ = Annotated[str | None, Query(description="opaque `next_cursor` from the previous page")]

Responses = dict[int | str, dict[str, Any]]
_E404: Responses = {404: {"model": ErrorResponse}}
_E409: Responses = {409: {"model": ErrorResponse}}
_E422: Responses = {422: {"model": ErrorResponse}}


def _svc(request: Request) -> TenantService:
    svc: TenantService = request.app.state.service
    return svc


def _cursor(token: str | None) -> Cursor | None:
    if token is None:
        return None
    try:
        return Cursor.decode(token)
    except InvalidCursor as exc:
        raise ValidationFailed("invalid pagination cursor", field="cursor") from exc


class _Keyed(Protocol):
    @property
    def id(self) -> UUID: ...
    @property
    def created_at(self) -> datetime: ...


def _next_cursor(rows: Sequence[_Keyed], limit: int) -> str | None:
    """We fetched limit+1 rows: if the extra one exists there is another page."""
    if len(rows) <= limit:
        return None
    last = rows[limit - 1]
    return Cursor(created_at=last.created_at, id=last.id).encode()


# --------------------------------------------------------------------------- tenants
@router.post(
    "/tenants",
    status_code=status.HTTP_201_CREATED,
    response_model=TenantOperation,
    responses={**_E409, **_E422},
    tags=["tenants"],
)
async def create_tenant(
    body: TenantCreate, request: Request, response: Response
) -> TenantOperation:
    tenant, task = await _svc(request).create_tenant(slug=body.slug, name=body.name)
    response.headers["Location"] = f"/v1/tenants/{tenant.id}"
    return TenantOperation(tenant=TenantOut.of(tenant), task=TaskOut.of(task))


@router.get("/tenants", response_model=TenantPage, responses=_E422, tags=["tenants"])
async def list_tenants(
    request: Request,
    status: TenantStatus | None = None,
    limit: Limit = 50,
    cursor: CursorQ = None,
) -> TenantPage:
    rows = await _svc(request).list_tenants(limit=limit + 1, after=_cursor(cursor), status=status)
    return TenantPage(
        items=[TenantOut.of(t) for t in rows[:limit]], next_cursor=_next_cursor(rows, limit)
    )


@router.get("/tenants/{tenant_id}", response_model=TenantOut, responses=_E404, tags=["tenants"])
async def get_tenant(tenant_id: UUID, request: Request) -> TenantOut:
    return TenantOut.of(await _svc(request).get_tenant(tenant_id))


@router.patch(
    "/tenants/{tenant_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TenantOperation,
    responses={**_E404, **_E409, **_E422},
    tags=["tenants"],
)
async def update_tenant(tenant_id: UUID, body: TenantPatch, request: Request) -> TenantOperation:
    tenant, task = await _svc(request).update_tenant(
        tenant_id, version=body.version, name=body.name
    )
    return TenantOperation(tenant=TenantOut.of(tenant), task=TaskOut.of(task))


@router.delete(
    "/tenants/{tenant_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TenantOperation,
    responses={**_E404, **_E409, **_E422},
    tags=["tenants"],
)
async def delete_tenant(
    tenant_id: UUID,
    request: Request,
    if_match: Annotated[
        str | None, Header(description="optional optimistic lock: the tenant version, e.g. `3`")
    ] = None,
) -> TenantOperation:
    version: int | None = None
    if if_match is not None:
        try:
            version = int(if_match.strip().strip('"'))
        except ValueError as exc:
            raise ValidationFailed(
                "If-Match must be the integer tenant version", field="If-Match"
            ) from exc
    tenant, task = await _svc(request).delete_tenant(tenant_id, version=version)
    return TenantOperation(tenant=TenantOut.of(tenant), task=TaskOut.of(task))


@router.get(
    "/tenants/{tenant_id}/tasks",
    response_model=TaskPage,
    responses={**_E404, **_E422},
    tags=["tasks"],
)
async def list_tenant_tasks(
    tenant_id: UUID,
    request: Request,
    status: TaskStatus | None = None,
    type: TaskType | None = None,
    limit: Limit = 50,
    cursor: CursorQ = None,
) -> TaskPage:
    return await list_tasks(
        request, tenant_id=tenant_id, status=status, type=type, limit=limit, cursor=cursor
    )


# ----------------------------------------------------------------------------- tasks
@router.get("/tasks", response_model=TaskPage, responses={**_E404, **_E422}, tags=["tasks"])
async def list_tasks(
    request: Request,
    tenant_id: UUID | None = None,
    status: TaskStatus | None = None,
    type: TaskType | None = None,
    limit: Limit = 50,
    cursor: CursorQ = None,
) -> TaskPage:
    rows = await _svc(request).list_tasks(
        limit=limit + 1, after=_cursor(cursor), tenant_id=tenant_id, status=status, type=type
    )
    return TaskPage(
        items=[TaskOut.of(t) for t in rows[:limit]], next_cursor=_next_cursor(rows, limit)
    )


@router.get("/tasks/{task_id}", response_model=TaskOut, responses=_E404, tags=["tasks"])
async def get_task(task_id: UUID, request: Request) -> TaskOut:
    return TaskOut.of(await _svc(request).get_task(task_id))
