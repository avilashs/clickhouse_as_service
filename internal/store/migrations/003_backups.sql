-- M5: backups, restore verification, and one-off jobs run by the cell agent.

-- where a cell keeps its backups, as the servers and the control plane reach it
ALTER TABLE cells ADD COLUMN IF NOT EXISTS backup_target jsonb;

CREATE TABLE IF NOT EXISTS backups (
    id              text PRIMARY KEY,
    cluster_id      text NOT NULL REFERENCES clusters(id),
    project_id      text NOT NULL,
    cell_id         text NOT NULL,
    kind            text NOT NULL CHECK (kind IN ('FULL','INCREMENTAL')),
    base_backup_id  text REFERENCES backups(id),
    state           text NOT NULL CHECK (state IN ('RUNNING','COMPLETED','FAILED','DELETING','DELETED')),
    path            text NOT NULL,                 -- object-store prefix: <project>/<cluster>/<id>
    scheduled       boolean NOT NULL DEFAULT false,
    size_bytes      bigint NOT NULL DEFAULT 0,
    error           text NOT NULL DEFAULT '',
    fingerprint     jsonb NOT NULL DEFAULT '{}',   -- per-table row counts and hashes around the backup
    created_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    verified_at     timestamptz                    -- set only by a passed restore verification
);
CREATE INDEX IF NOT EXISTS backups_cluster ON backups (cluster_id, created_at DESC);

CREATE TABLE IF NOT EXISTS restore_verifications (
    id                 text PRIMARY KEY,
    backup_id          text NOT NULL REFERENCES backups(id),
    scratch_cluster_id text NOT NULL DEFAULT '',
    state              text NOT NULL CHECK (state IN ('RUNNING','PASSED','FAILED')),
    detail             jsonb NOT NULL DEFAULT '{}',
    scheduled          boolean NOT NULL DEFAULT false,
    created_at         timestamptz NOT NULL DEFAULT now(),
    finished_at        timestamptz
);
CREATE INDEX IF NOT EXISTS restore_verifications_backup ON restore_verifications (backup_id, created_at DESC);

-- A job is work the cell agent runs against a cluster's servers once (backup, restore) and reports
-- on. It is served to the agent beside the desired state but is not part of it, so starting a job
-- does not change the desired-state hash. Secret placeholders in spec are resolved when served.
CREATE TABLE IF NOT EXISTS cluster_jobs (
    id          text PRIMARY KEY,
    cluster_id  text NOT NULL,
    cell_id     text NOT NULL,
    kind        text NOT NULL,
    spec        jsonb NOT NULL,
    state       text NOT NULL CHECK (state IN ('PENDING','RUNNING','SUCCEEDED','FAILED')),
    message     text NOT NULL DEFAULT '',
    result      jsonb NOT NULL DEFAULT '{}',
    served      boolean NOT NULL DEFAULT true,     -- false once the workflow has collected the result
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cluster_jobs_cell ON cluster_jobs (cell_id) WHERE served;

-- parameters of an operation that needs more than the cluster id (restore: which backup)
ALTER TABLE operations ADD COLUMN IF NOT EXISTS params jsonb NOT NULL DEFAULT '{}';
