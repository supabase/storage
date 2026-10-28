-- The AFTER UPDATE OF trigger already targets lifecycle columns. Enforce the
-- role even for no-op writes: OrioleDB can supply identical OLD/NEW images for
-- a changed committed row so those images cannot decide whether to check it.
CREATE OR REPLACE FUNCTION storage.enforce_bucket_lifecycle_service_role()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
  IF current_user::text IS DISTINCT FROM TG_ARGV[0] THEN
    -- Keep this check AFTER caller RLS accepts the proposed row. The API
    -- recognizes this exact signal after rolling back its permission probe.
    RAISE EXCEPTION 'bucket control columns may only be changed by the configured storage service role'
      USING ERRCODE = 'PST01',
            SCHEMA = TG_TABLE_SCHEMA,
            TABLE = TG_TABLE_NAME,
            CONSTRAINT = TG_NAME;
  END IF;

  RETURN NULL;
END;
$$;
