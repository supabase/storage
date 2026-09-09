-- Add lifecycle execution controls and durable shard state to the versioning schema.

ALTER TABLE storage.buckets
  ADD COLUMN IF NOT EXISTS lifecycle_shard_epoch bigint NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS lifecycle_shard_count integer NOT NULL DEFAULT 1;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_constraint
    WHERE conrelid = 'storage.buckets'::regclass
      AND conname = 'buckets_lifecycle_shard_topology_check'
  ) THEN
    ALTER TABLE storage.buckets
    ADD CONSTRAINT buckets_lifecycle_shard_topology_check
      CHECK (lifecycle_shard_epoch = 1 AND lifecycle_shard_count = 1) NOT VALID;
  END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS storage.bucket_lifecycle_states (
    bucket_id text NOT NULL,
    scan_kind text NOT NULL DEFAULT 'NONCURRENT',
    shard_id integer NOT NULL,
    shard_epoch bigint NOT NULL,
    shard_count integer NOT NULL,
    next_run_at timestamptz,
    claim_id uuid,
    claim_until timestamptz,
    continuation jsonb,
    last_started_at timestamptz,
    last_completed_at timestamptz,
    last_success_at timestamptz,
    last_result jsonb,
    failure_count integer NOT NULL DEFAULT 0,
    last_error jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (bucket_id, scan_kind, shard_id),

    CONSTRAINT bucket_lifecycle_states_bucket_id_fkey
      FOREIGN KEY (bucket_id)
      REFERENCES storage.buckets(id),

    CONSTRAINT bucket_lifecycle_scan_kind_check
      CHECK (scan_kind IN ('NONCURRENT', 'CURRENT')),

    CONSTRAINT bucket_lifecycle_shard_shape_check
      CHECK (
        shard_epoch >= 1
        AND shard_count BETWEEN 1 AND 1024
        AND shard_id >= 0
        AND shard_id < shard_count
      ),

    CONSTRAINT bucket_lifecycle_failure_count_check
      CHECK (failure_count >= 0),

    CONSTRAINT bucket_lifecycle_continuation_object_check
      CHECK (
        continuation IS NULL
        OR jsonb_typeof(continuation) = 'object'
      ),

    CONSTRAINT bucket_lifecycle_recovery_due_check
      CHECK (
        next_run_at IS NOT NULL
        OR NOT jsonb_path_exists(
          COALESCE(continuation, '{}'::jsonb),
          '$.batch.inFlight'
        )
      ),

    CONSTRAINT bucket_lifecycle_last_result_object_check
      CHECK (
        last_result IS NULL
        OR jsonb_typeof(last_result) = 'object'
      )
);

CREATE INDEX IF NOT EXISTS bucket_lifecycle_states_due_idx
ON storage.bucket_lifecycle_states (
    next_run_at,
    bucket_id,
    scan_kind,
    shard_id
)
WHERE next_run_at IS NOT NULL;

ALTER TABLE storage.bucket_lifecycle_states ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE
    anon_role text = COALESCE(current_setting('storage.anon_role', true), 'anon');
    authenticated_role text = COALESCE(current_setting('storage.authenticated_role', true), 'authenticated');
    service_role text = COALESCE(current_setting('storage.service_role', true), 'service_role');
    super_user text = COALESCE(current_setting('storage.super_user', true), 'postgres');
    role_name text;
BEGIN
    REVOKE ALL ON TABLE storage.bucket_lifecycle_states FROM PUBLIC;

    FOREACH role_name IN ARRAY ARRAY[anon_role, authenticated_role]
    LOOP
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
        EXECUTE format('REVOKE ALL ON TABLE storage.bucket_lifecycle_states FROM %I', role_name);
      END IF;
    END LOOP;

    FOREACH role_name IN ARRAY ARRAY['postgres', super_user, service_role]
    LOOP
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
        EXECUTE format('GRANT ALL ON TABLE storage.bucket_lifecycle_states TO %I', role_name);
      END IF;
    END LOOP;
END;
$$;
