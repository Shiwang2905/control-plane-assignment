-- Schema for the tenant control plane.
-- The database, not application code, is the last line of defence for every invariant:
-- slug uniqueness, allowed enum values, and "at most one open task per tenant".

CREATE TABLE tenants (
    id          uuid        PRIMARY KEY,
    slug        text        NOT NULL,
    name        text        NOT NULL,
    status      text        NOT NULL,
    version     integer     NOT NULL DEFAULT 1,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    -- Unique across ALL rows, including destroyed tenants (we soft-delete, never DELETE).
    -- Concurrent creates with the same slug: Postgres lets exactly one INSERT win;
    -- the other gets a unique_violation which the API maps to tenant_already_exists.
    CONSTRAINT tenants_slug_key    UNIQUE (slug),
    CONSTRAINT tenants_slug_format CHECK (slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'),
    CONSTRAINT tenants_name_check  CHECK (length(btrim(name)) > 0),
    CONSTRAINT tenants_version_check CHECK (version >= 1),
    CONSTRAINT tenants_status_check CHECK (status IN
        ('provisioning', 'active', 'updating', 'destroying', 'destroyed', 'failed'))
);
-- Keyset pagination: ORDER BY created_at, id.
CREATE INDEX tenants_created_idx ON tenants (created_at, id);
CREATE INDEX tenants_status_idx  ON tenants (status, created_at, id);

CREATE TABLE tasks (
    id          uuid        PRIMARY KEY,           -- doubles as the published event id
    tenant_id   uuid        NOT NULL REFERENCES tenants (id),
    type        text        NOT NULL,
    status      text        NOT NULL,
    error       text,                               -- worker-supplied reason on failure
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    CONSTRAINT tasks_type_check   CHECK (type IN ('deploy', 'update', 'destroy')),
    CONSTRAINT tasks_status_check CHECK (status IN ('accepted', 'in_progress', 'done', 'failed'))
);
CREATE INDEX tasks_created_idx ON tasks (created_at, id);
CREATE INDEX tasks_tenant_idx  ON tasks (tenant_id, created_at, id);
-- Belt and braces: the tenant status guards already imply this, but if a bug ever
-- slipped past them the database refuses a second open task for the same tenant.
CREATE UNIQUE INDEX tasks_one_open_per_tenant
    ON tasks (tenant_id) WHERE status IN ('accepted', 'in_progress');

-- Transactional outbox: rows are written in the SAME transaction as the tenant/task
-- change, and a relay publishes them to Kafka only after that transaction commits.
CREATE TABLE outbox (
    id           bigserial   PRIMARY KEY,           -- publish order
    event_id     uuid        NOT NULL UNIQUE,       -- = task id
    topic        text        NOT NULL,
    key          text        NOT NULL,              -- Kafka partition key (tenant id)
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Inbound idempotency: one row per worker update message we have handled.
CREATE TABLE processed_messages (
    message_id   text        PRIMARY KEY,
    task_id      uuid,
    outcome      text        NOT NULL,              -- pending -> applied | stale | rejected
    processed_at timestamptz NOT NULL DEFAULT now()
);
