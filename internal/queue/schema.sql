CREATE TABLE IF NOT EXISTS jobs (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 idempotency_key text NOT NULL UNIQUE,
 duration_ms integer NOT NULL CHECK (duration_ms BETWEEN 0 AND 30000),
 simulate_failure boolean NOT NULL DEFAULT false,
 state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','running','succeeded','failed','cancelled')),
 token bigint NOT NULL DEFAULT 0,
 attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
 lease_until timestamptz,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS jobs_claim ON jobs(state, lease_until, id) WHERE state IN ('queued','running');
CREATE TABLE IF NOT EXISTS job_effects (
 job_id bigint PRIMARY KEY REFERENCES jobs(id),
 token bigint NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
