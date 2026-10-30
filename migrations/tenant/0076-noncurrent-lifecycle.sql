-- Add durable lifecycle execution state with one row per bucket.

CREATE TABLE IF NOT EXISTS storage.bucket_lifecycle_states (
    bucket_id text PRIMARY KEY,
    generation uuid,
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

    CONSTRAINT bucket_lifecycle_states_bucket_id_fkey
      FOREIGN KEY (bucket_id)
      REFERENCES storage.buckets(id)
      ON DELETE CASCADE,

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

DO $$
BEGIN
    IF NOT (SELECT relrowsecurity FROM pg_catalog.pg_class
            WHERE oid = 'storage.bucket_lifecycle_states'::regclass) THEN
        ALTER TABLE storage.bucket_lifecycle_states ENABLE ROW LEVEL SECURITY;
    END IF;
    IF to_regclass('storage.bucket_lifecycle_states_due_idx') IS NULL THEN
        CREATE INDEX IF NOT EXISTS bucket_lifecycle_states_due_idx
        ON storage.bucket_lifecycle_states (next_run_at, bucket_id)
        WHERE next_run_at IS NOT NULL;
    END IF;
END;
$$;

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

    FOREACH role_name IN ARRAY ARRAY[super_user, service_role]
    LOOP
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
        EXECUTE format('GRANT ALL ON TABLE storage.bucket_lifecycle_states TO %I', role_name);
      END IF;
    END LOOP;
END;
$$;
