-- A committed deletion journal remains authoritative until recovery completes.
-- Enforce this in the database so older writers also cannot promote its targets.
CREATE OR REPLACE FUNCTION storage.prevent_lifecycle_promotion()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM storage.bucket_lifecycle_states AS state
        WHERE state.bucket_id = OLD.bucket_id
          AND state.continuation #> '{batch,inFlight}' IS NOT NULL
          AND state.continuation @> jsonb_build_object(
              'batch', jsonb_build_object('versions', jsonb_build_array(
                  jsonb_build_object('name', OLD.name, 'version', OLD.version)
              ))
          )
    ) THEN
        RAISE EXCEPTION 'Object version has lifecycle deletion in flight'
            USING ERRCODE = '55P03';
    END IF;
    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION storage.prevent_lifecycle_promotion() FROM PUBLIC;

DROP TRIGGER IF EXISTS prevent_lifecycle_promotion ON storage.objects;
CREATE TRIGGER prevent_lifecycle_promotion
BEFORE UPDATE OF archived_at ON storage.objects
FOR EACH ROW
WHEN (
    OLD.archived_at IS NOT NULL AND NEW.archived_at IS NULL
    AND NEW.bucket_id = OLD.bucket_id AND NEW.name = OLD.name
    AND NEW.version IS NOT DISTINCT FROM OLD.version
)
EXECUTE FUNCTION storage.prevent_lifecycle_promotion();

-- Builds that predate lifecycle state do not clear it when deleting a bucket.
-- Only empty continuations are safe to remove without a decoder. Structured state
-- blocks via the foreign key until a lifecycle-aware writer validates and clears it.
CREATE OR REPLACE FUNCTION storage.delete_idle_lifecycle_states()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    DELETE FROM storage.bucket_lifecycle_states
    WHERE bucket_id = OLD.id
      AND continuation IS NULL;
    RETURN OLD;
END;
$$;

REVOKE ALL ON FUNCTION storage.delete_idle_lifecycle_states() FROM PUBLIC;

DROP TRIGGER IF EXISTS delete_idle_lifecycle_states ON storage.buckets;
CREATE TRIGGER delete_idle_lifecycle_states
BEFORE DELETE ON storage.buckets
FOR EACH ROW
EXECUTE FUNCTION storage.delete_idle_lifecycle_states();

-- Versioning can be enabled through either the API or a direct bucket update.
-- Wake dormant policies without resetting a healthy schedule or recovery journal.
CREATE OR REPLACE FUNCTION storage.wake_lifecycle_on_versioning_enable()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    INSERT INTO storage.bucket_lifecycle_states (
        bucket_id, scan_kind, shard_id, shard_epoch, shard_count, next_run_at
    ) VALUES (
        NEW.id, 'NONCURRENT', 0, NEW.lifecycle_shard_epoch, NEW.lifecycle_shard_count,
        CASE WHEN NEW.lifecycle_configuration @> '{"rules":[{"status":"Enabled"}]}'::jsonb
             THEN clock_timestamp() END
    )
    ON CONFLICT (bucket_id, scan_kind, shard_id) DO UPDATE
    SET next_run_at = EXCLUDED.next_run_at, updated_at = clock_timestamp()
    WHERE bucket_lifecycle_states.next_run_at IS NULL
      AND EXCLUDED.next_run_at IS NOT NULL;
    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION storage.wake_lifecycle_on_versioning_enable() FROM PUBLIC;

DROP TRIGGER IF EXISTS wake_lifecycle_on_versioning_enable ON storage.buckets;
CREATE TRIGGER wake_lifecycle_on_versioning_enable
AFTER UPDATE OF versioning_status ON storage.buckets
FOR EACH ROW
WHEN (
    OLD.versioning_status = 'DISABLED' AND NEW.versioning_status = 'ENABLED'
    AND NEW.lifecycle_configuration IS NOT NULL
)
EXECUTE FUNCTION storage.wake_lifecycle_on_versioning_enable();

-- Policies existed before the execution tables. Also repair policies stranded by
-- an earlier versioning transition, preserving all existing due/claimed work.
INSERT INTO storage.bucket_lifecycle_states (
    bucket_id, scan_kind, shard_id, shard_epoch, shard_count, next_run_at
)
SELECT id, 'NONCURRENT', 0, lifecycle_shard_epoch, lifecycle_shard_count,
       CASE WHEN versioning_status <> 'DISABLED'
                 AND lifecycle_configuration @> '{"rules":[{"status":"Enabled"}]}'::jsonb
            THEN clock_timestamp() END
FROM storage.buckets
WHERE type = 'STANDARD' AND lifecycle_configuration IS NOT NULL
ON CONFLICT (bucket_id, scan_kind, shard_id) DO UPDATE
SET next_run_at = EXCLUDED.next_run_at, updated_at = clock_timestamp()
WHERE bucket_lifecycle_states.next_run_at IS NULL
  AND EXCLUDED.next_run_at IS NOT NULL;
