-- Basalt control-plane schema (design §4). Secret values are never stored here.

CREATE TABLE IF NOT EXISTS cells (
    id                  text PRIMARY KEY,
    region              text NOT NULL,
    state               text NOT NULL CHECK (state IN ('ACTIVE','DRAINING','SUSPENDED')),
    profile             text NOT NULL CHECK (profile IN ('local','cloud')),
    domain              text NOT NULL,
    max_clusters        int  NOT NULL,
    agent_token_sha256  text NOT NULL,
    ca_pem              text NOT NULL DEFAULT '',
    desired_generation  bigint NOT NULL DEFAULT 0,
    agent_last_seen_at  timestamptz
);

CREATE TABLE IF NOT EXISTS cell_capacity (
    cell_id           text NOT NULL REFERENCES cells(id),
    pool              text NOT NULL,
    milli_cpu_total   bigint NOT NULL,
    memory_total      bigint NOT NULL,
    pods_total        int NOT NULL,
    PRIMARY KEY (cell_id, pool)
);

CREATE TABLE IF NOT EXISTS clusters (
    id                    text PRIMARY KEY,
    project_id            text NOT NULL,
    cell_id               text NOT NULL REFERENCES cells(id),
    name                  text NOT NULL,
    engine                text NOT NULL,
    state                 text NOT NULL CHECK (state IN ('CREATING','ACTIVE','UPDATING','DEGRADED','FAILED','DELETING','DELETED')),
    state_reason          text NOT NULL DEFAULT '',
    current_spec_version  int NOT NULL DEFAULT 0,
    desired_spec_version  int NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    deleted_at            timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS clusters_project_name ON clusters (project_id, name) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS cluster_specs (
    cluster_id  text NOT NULL REFERENCES clusters(id),
    version     int  NOT NULL,
    spec        jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cluster_id, version)
);

CREATE TABLE IF NOT EXISTS operations (
    id          text PRIMARY KEY,
    project_id  text NOT NULL,
    cluster_id  text NOT NULL REFERENCES clusters(id),
    kind        text NOT NULL,
    state       text NOT NULL CHECK (state IN ('PENDING','RUNNING','SUCCEEDED','FAILED')),
    error       text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
-- per-resource lock at the data layer: at most one unfinished operation per cluster
CREATE UNIQUE INDEX IF NOT EXISTS operations_one_active ON operations (cluster_id) WHERE state IN ('PENDING','RUNNING');

CREATE TABLE IF NOT EXISTS idempotency_keys (
    project_id    text NOT NULL,
    key           text NOT NULL,
    request_hash  text NOT NULL,
    operation_id  text NOT NULL REFERENCES operations(id),
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, key)
);

CREATE TABLE IF NOT EXISTS capacity_reservations (
    operation_id  text PRIMARY KEY,             -- one reservation per operation: retries are no-ops
    cell_id       text NOT NULL,
    cluster_id    text NOT NULL,
    pool          text NOT NULL,
    milli_cpu     bigint NOT NULL,
    memory        bigint NOT NULL,
    pods          int NOT NULL,
    state         text NOT NULL CHECK (state IN ('HELD','COMMITTED','RELEASED')),
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Desired state per cluster as served to the cell agent. Payload contains secret
-- references, never values. generation comes from cells.desired_generation.
CREATE TABLE IF NOT EXISTS desired_clusters (
    cluster_id    text PRIMARY KEY REFERENCES clusters(id),
    cell_id       text NOT NULL REFERENCES cells(id),
    generation    bigint NOT NULL,
    payload_hash  text NOT NULL,
    tombstone     boolean NOT NULL DEFAULT false,
    payload       jsonb NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS desired_clusters_cell ON desired_clusters (cell_id, generation);

CREATE TABLE IF NOT EXISTS observed_clusters (
    cluster_id   text PRIMARY KEY,
    cell_id      text NOT NULL,
    report       jsonb NOT NULL,
    observed_at  timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id          bigserial PRIMARY KEY,
    at          timestamptz NOT NULL DEFAULT now(),
    actor       text NOT NULL,
    action      text NOT NULL,
    resource    text NOT NULL,
    request_id  text NOT NULL DEFAULT '',
    details     jsonb NOT NULL DEFAULT '{}'
);
