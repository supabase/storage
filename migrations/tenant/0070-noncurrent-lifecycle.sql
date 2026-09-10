-- Add lifecycle execution controls and durable shard state to the versioning schema.

ALTER TABLE storage.buckets
  ADD COLUMN IF NOT EXISTS lifecycle_shard_epoch bigint NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS lifecycle_shard_count integer NOT NULL DEFAULT 1;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_constraint
    WHERE conrelid = 'storage.buckets'::regclass
      AND conname = 'buckets_lifecycle_shard_epoch_check'
  ) THEN
    ALTER TABLE storage.buckets
    ADD CONSTRAINT buckets_lifecycle_shard_epoch_check
      CHECK (lifecycle_shard_epoch >= 1) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_constraint
    WHERE conrelid = 'storage.buckets'::regclass
      AND conname = 'buckets_lifecycle_shard_count_check'
  ) THEN
    ALTER TABLE storage.buckets
    ADD CONSTRAINT buckets_lifecycle_shard_count_check
      CHECK (lifecycle_shard_count BETWEEN 1 AND 1024) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_constraint
    WHERE conrelid = 'storage.buckets'::regclass
      AND conname = 'buckets_lifecycle_standard_only_check'
  ) THEN
    ALTER TABLE storage.buckets
    ADD CONSTRAINT buckets_lifecycle_standard_only_check CHECK (
      type = 'STANDARD'
      OR (
        lifecycle_shard_epoch = 1
        AND lifecycle_shard_count = 1
      )
    ) NOT VALID;
  END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS storage.bucket_lifecycle_states (
    bucket_id text NOT NULL,
    scan_kind text NOT NULL DEFAULT 'NONCURRENT',
    shard_id integer NOT NULL,
    shard_epoch bigint NOT NULL,
    shard_count integer NOT NULL,
    configuration_generation uuid NOT NULL,
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
      REFERENCES storage.buckets(id)
      ON DELETE RESTRICT,

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

CREATE INDEX IF NOT EXISTS bucket_lifecycle_states_expired_claim_idx
ON storage.bucket_lifecycle_states (
    claim_until,
    bucket_id,
    scan_kind,
    shard_id
)
WHERE claim_until IS NOT NULL;

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

CREATE OR REPLACE FUNCTION storage.protect_bucket_control_columns()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    service_role text = TG_ARGV[0];
    configuration_changed boolean;
    topology_changed boolean;
    changed_group_count integer;
BEGIN
    IF TG_OP = 'INSERT' THEN
      IF NEW.lifecycle_shard_epoch IS DISTINCT FROM 1::bigint
         OR NEW.lifecycle_shard_count IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION 'bucket control columns must use their protected defaults on insert'
          USING ERRCODE = '42501';
      END IF;

      IF (NEW.lifecycle_configuration IS NOT NULL OR NEW.lifecycle_configuration_generation IS NOT NULL)
         AND NOT pg_has_role(current_user, service_role, 'MEMBER') THEN
        RAISE EXCEPTION 'only members of the configured storage service role may insert lifecycle policy state'
          USING ERRCODE = '42501',
                HINT = format('Insert with both lifecycle columns NULL and configure lifecycle through the Storage API afterward, or insert as a member of %I.', service_role);
      END IF;
      RETURN NEW;
    END IF;

    configuration_changed =
      OLD.lifecycle_configuration IS DISTINCT FROM NEW.lifecycle_configuration
      OR OLD.lifecycle_configuration_generation IS DISTINCT FROM NEW.lifecycle_configuration_generation;
    topology_changed =
      OLD.lifecycle_shard_epoch IS DISTINCT FROM NEW.lifecycle_shard_epoch
      OR OLD.lifecycle_shard_count IS DISTINCT FROM NEW.lifecycle_shard_count;

    changed_group_count =
      configuration_changed::integer
      + topology_changed::integer;

    IF changed_group_count = 0 THEN
      RETURN NEW;
    END IF;

    IF NEW.type IS DISTINCT FROM 'STANDARD' THEN
      RAISE EXCEPTION 'bucket lifecycle controls require a Standard bucket'
        USING ERRCODE = '0A000';
    END IF;

    IF changed_group_count > 1 THEN
      RAISE EXCEPTION 'bucket control groups must be changed by separate operations'
        USING ERRCODE = '22023';
    END IF;

    IF topology_changed THEN
      RAISE EXCEPTION 'lifecycle shard topology changes are not supported in v1'
        USING ERRCODE = '0A000';
    END IF;

    IF configuration_changed THEN
      IF NEW.lifecycle_configuration IS NULL
         AND NEW.lifecycle_configuration_generation IS NULL THEN
        RETURN NEW;
      END IF;

      IF NEW.lifecycle_configuration IS NULL
         OR NEW.lifecycle_configuration_generation IS NULL
         OR OLD.lifecycle_configuration IS NOT DISTINCT FROM NEW.lifecycle_configuration
         OR OLD.lifecycle_configuration_generation IS NOT DISTINCT FROM NEW.lifecycle_configuration_generation THEN
        RAISE EXCEPTION 'a changed lifecycle policy requires a new non-null generation'
          USING ERRCODE = '22023';
      END IF;

      -- The control-plane AFTER trigger enforces service-role writes after caller RLS.
      RETURN NEW;
    END IF;

    RETURN NEW;
END;
$$;

DO $$
DECLARE
    service_role text = COALESCE(current_setting('storage.service_role', true), 'service_role');
BEGIN
    DROP TRIGGER IF EXISTS protect_bucket_control_insert ON storage.buckets;
    EXECUTE format(
      'CREATE TRIGGER protect_bucket_control_insert BEFORE INSERT ON storage.buckets FOR EACH ROW EXECUTE FUNCTION storage.protect_bucket_control_columns(%L)',
      service_role
    );

    DROP TRIGGER IF EXISTS protect_bucket_control_update ON storage.buckets;
    EXECUTE format(
      'CREATE TRIGGER protect_bucket_control_update BEFORE UPDATE OF lifecycle_configuration, lifecycle_configuration_generation, lifecycle_shard_epoch, lifecycle_shard_count ON storage.buckets FOR EACH ROW EXECUTE FUNCTION storage.protect_bucket_control_columns(%L)',
      service_role
    );
END;
$$;
