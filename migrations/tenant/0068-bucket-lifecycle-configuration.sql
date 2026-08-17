-- Add lifecycle configuration.

ALTER TABLE storage.buckets
ADD COLUMN IF NOT EXISTS lifecycle_configuration jsonb,
ADD COLUMN IF NOT EXISTS lifecycle_configuration_generation uuid;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint
    WHERE conrelid = 'storage.buckets'::regclass
      AND conname = 'buckets_lifecycle_configuration_pair_check'
  ) THEN
    ALTER TABLE storage.buckets
    ADD CONSTRAINT buckets_lifecycle_configuration_pair_check CHECK (
      (lifecycle_configuration IS NULL) =
      (lifecycle_configuration_generation IS NULL)
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint
    WHERE conrelid = 'storage.buckets'::regclass
      AND conname = 'buckets_lifecycle_configuration_shape_check'
  ) THEN
    ALTER TABLE storage.buckets
    ADD CONSTRAINT buckets_lifecycle_configuration_shape_check CHECK (
      lifecycle_configuration IS NULL
      OR (
        jsonb_typeof(lifecycle_configuration) = 'object'
        AND lifecycle_configuration ? 'rules'
        AND CASE
          WHEN jsonb_typeof(lifecycle_configuration -> 'rules') = 'array'
            THEN jsonb_array_length(lifecycle_configuration -> 'rules') BETWEEN 1 AND 1000
          ELSE false
        END
      )
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint
    WHERE conrelid = 'storage.buckets'::regclass
      AND conname = 'buckets_lifecycle_configuration_standard_only_check'
  ) THEN
    ALTER TABLE storage.buckets
    ADD CONSTRAINT buckets_lifecycle_configuration_standard_only_check CHECK (
      type = 'STANDARD'
      OR (
        lifecycle_configuration IS NULL
        AND lifecycle_configuration_generation IS NULL
      )
    );
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION storage.protect_bucket_control_columns()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
  service_role text = TG_ARGV[0];
  current_operation text = COALESCE(current_setting('storage.operation', true), '');
  configuration_changed boolean;
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.lifecycle_configuration IS NOT NULL
       OR NEW.lifecycle_configuration_generation IS NOT NULL THEN
      RAISE EXCEPTION 'bucket control columns must use their protected defaults on insert'
        USING ERRCODE = '42501';
    END IF;

    RETURN NEW;
  END IF;

  configuration_changed =
    OLD.lifecycle_configuration IS DISTINCT FROM NEW.lifecycle_configuration
    OR OLD.lifecycle_configuration_generation IS DISTINCT FROM NEW.lifecycle_configuration_generation;

  IF NOT configuration_changed THEN
    RETURN NEW;
  END IF;

  IF NEW.type IS DISTINCT FROM 'STANDARD' THEN
    RAISE EXCEPTION 'bucket versioning and lifecycle controls require a Standard bucket'
      USING ERRCODE = '0A000';
  END IF;

  IF current_user::text IS DISTINCT FROM service_role THEN
    RAISE EXCEPTION 'bucket control columns may only be changed by the configured storage service role'
      USING ERRCODE = '42501';
  END IF;

  IF NEW.lifecycle_configuration IS NULL
     AND NEW.lifecycle_configuration_generation IS NULL THEN
    IF current_operation NOT IN (
      'storage.s3.bucket.delete_lifecycle',
      'storage.bucket.delete_lifecycle'
    ) THEN
      RAISE EXCEPTION 'invalid operation for lifecycle configuration deletion'
        USING ERRCODE = '42501';
    END IF;

    RETURN NEW;
  END IF;

  IF current_operation NOT IN (
    'storage.s3.bucket.put_lifecycle',
    'storage.bucket.put_lifecycle'
  ) THEN
    RAISE EXCEPTION 'invalid operation for lifecycle configuration update'
      USING ERRCODE = '42501';
  END IF;

  IF NEW.lifecycle_configuration IS NULL
     OR NEW.lifecycle_configuration_generation IS NULL
     OR OLD.lifecycle_configuration IS NOT DISTINCT FROM NEW.lifecycle_configuration
     OR OLD.lifecycle_configuration_generation IS NOT DISTINCT FROM NEW.lifecycle_configuration_generation THEN
    RAISE EXCEPTION 'a changed lifecycle policy requires a new non-null generation'
      USING ERRCODE = '22023';
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
    'CREATE TRIGGER protect_bucket_control_update BEFORE UPDATE OF lifecycle_configuration, lifecycle_configuration_generation ON storage.buckets FOR EACH ROW EXECUTE FUNCTION storage.protect_bucket_control_columns(%L)',
    service_role
  );
END;
$$;
