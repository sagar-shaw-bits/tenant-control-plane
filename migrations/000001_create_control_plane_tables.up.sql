CREATE TABLE tenants (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('provisioning', 'active', 'updating', 'destroying', 'destroyed', 'failed')
    ),
    version BIGINT NOT NULL CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tenants_slug_format CHECK (slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'),
    CONSTRAINT tenants_name_not_empty CHECK (length(btrim(name)) > 0)
);

CREATE INDEX tenants_created_at_id_idx ON tenants (created_at DESC, id DESC);
CREATE INDEX tenants_status_created_at_id_idx ON tenants (status, created_at DESC, id DESC);

CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    type TEXT NOT NULL CHECK (type IN ('deploy', 'update', 'destroy')),
    tenant_id UUID NOT NULL REFERENCES tenants (id),
    status TEXT NOT NULL CHECK (status IN ('accepted', 'in_progress', 'done', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX tasks_one_open_per_tenant_idx
    ON tasks (tenant_id)
    WHERE status IN ('accepted', 'in_progress');
CREATE INDEX tasks_created_at_id_idx ON tasks (created_at DESC, id DESC);
CREATE INDEX tasks_tenant_created_at_id_idx
    ON tasks (tenant_id, created_at DESC, id DESC);
CREATE INDEX tasks_status_created_at_id_idx
    ON tasks (status, created_at DESC, id DESC);

CREATE TABLE outbox (
    id UUID PRIMARY KEY REFERENCES tasks (id),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_created_at_id_idx
    ON outbox (created_at, id)
    WHERE published_at IS NULL;

CREATE TABLE processed_updates (
    update_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
