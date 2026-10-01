package queue

const schema = `
CREATE TABLE IF NOT EXISTS workers (
    id text PRIMARY KEY,
    concurrency integer NOT NULL CHECK (concurrency BETWEEN 1 AND 32),
    seen_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    id uuid PRIMARY KEY,
    sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    kind text NOT NULL,
    queue text NOT NULL,
    payload jsonb NOT NULL,
    request_hash text NOT NULL,
    idempotency_key text UNIQUE,
    priority integer NOT NULL CHECK (priority BETWEEN 0 AND 9),
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','succeeded','dead','canceled')),
    attempt integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    timeout_seconds integer NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 300),
    worker_id text,
    lease_until timestamptz,
    available_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    result jsonb,
    error text NOT NULL DEFAULT '',
    CHECK ((status = 'running') = (worker_id IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (queue, priority DESC, available_at, sequence) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_expired ON jobs (lease_until) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS jobs_terminal_age ON jobs(updated_at) WHERE status IN ('succeeded','dead','canceled');
CREATE TABLE IF NOT EXISTS attempts (
    job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    number integer NOT NULL,
    worker_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('running','succeeded','failed','expired','canceled')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    error text NOT NULL DEFAULT '',
    PRIMARY KEY (job_id, number)
);

CREATE TABLE IF NOT EXISTS job_events (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 job jsonb NOT NULL,
 previous text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS job_events_age ON job_events(created_at);
CREATE OR REPLACE FUNCTION record_job_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP = 'INSERT' OR OLD.status IS DISTINCT FROM NEW.status THEN
  -- Serialize allocation until commit so a later event cannot overtake an uncommitted one.
  PERFORM pg_advisory_xact_lock(7241074);
  INSERT INTO job_events(job,previous) VALUES(to_jsonb(NEW),CASE WHEN TG_OP='INSERT' THEN '' ELSE OLD.status END);
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS job_event ON jobs;
CREATE TRIGGER job_event AFTER INSERT OR UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION record_job_event();
`
