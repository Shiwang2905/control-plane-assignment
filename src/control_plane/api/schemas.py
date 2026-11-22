"""HTTP request/response models (the public JSON contract)."""

from __future__ import annotations

from typing import Annotated, Self
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from control_plane.domain import SLUG_RE, TaskStatus, TaskType, TenantStatus
from control_plane.repository import Task, Tenant
from control_plane.timeutil import iso_ms

Name = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")  # unknown fields (e.g. "slug" on PATCH) -> 422


# ----------------------------------------------------------------------- requests
class TenantCreate(_Strict):
    slug: str = Field(examples=["acme-prod"])
    name: Name = Field(examples=["Acme Corp (production)"])

    @field_validator("slug")
    @classmethod
    def _slug_format(cls, v: str) -> str:
        # fullmatch, not match: `$` in `re.match` also accepts a trailing "\n".
        if not SLUG_RE.fullmatch(v):
            raise ValueError("slug must be 3-28 chars matching ^[a-z][-a-z0-9]{1,26}[a-z0-9]$")
        return v


class TenantPatch(_Strict):
    """Partial update. `version` is mandatory: it is the optimistic-lock token."""

    version: int = Field(ge=1, examples=[1])
    name: Name | None = Field(default=None, examples=["Acme Corp"])

    @model_validator(mode="after")
    def _has_changes(self) -> Self:
        if self.name is None:
            raise ValueError("at least one updatable field (name) must be provided")
        return self


# ---------------------------------------------------------------------- responses
class TenantOut(BaseModel):
    id: UUID
    slug: str
    name: str
    status: TenantStatus
    version: int
    created_at: str
    updated_at: str

    @classmethod
    def of(cls, t: Tenant) -> TenantOut:
        return cls(
            id=t.id,
            slug=t.slug,
            name=t.name,
            status=t.status,
            version=t.version,
            created_at=iso_ms(t.created_at),
            updated_at=iso_ms(t.updated_at),
        )


class TaskOut(BaseModel):
    id: UUID
    type: TaskType
    tenant_id: UUID
    status: TaskStatus
    error: str | None
    created_at: str
    updated_at: str

    @classmethod
    def of(cls, t: Task) -> TaskOut:
        return cls(
            id=t.id,
            type=t.type,
            tenant_id=t.tenant_id,
            status=t.status,
            error=t.error,
            created_at=iso_ms(t.created_at),
            updated_at=iso_ms(t.updated_at),
        )


class TenantOperation(BaseModel):
    """Returned by create/update/delete: the tenant *and* the async task it started."""

    tenant: TenantOut
    task: TaskOut


class TenantPage(BaseModel):
    items: list[TenantOut]
    next_cursor: str | None


class TaskPage(BaseModel):
    items: list[TaskOut]
    next_cursor: str | None


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict[str, object] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorBody
