-- M2: sandbox runtime for shared-tier servers, per cell (ADR-0003)
ALTER TABLE cells ADD COLUMN IF NOT EXISTS sandbox_runtime text NOT NULL DEFAULT '';
