ALTER TABLE storage.objects
    ADD COLUMN IF NOT EXISTS signature bytea;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'objects_signature_length'
          AND conrelid = 'storage.objects'::regclass
    ) THEN
        ALTER TABLE storage.objects
            ADD CONSTRAINT objects_signature_length
            CHECK (signature IS NULL OR octet_length(signature) = 32)
            NOT VALID;
    END IF;
END $$;

DROP TRIGGER IF EXISTS update_objects_updated_at ON storage.objects;

-- Keep this list in sync with all non-generated storage.objects columns except signature.
-- Generated columns such as path_tokens are intentionally omitted.
-- The explicit row comparison avoids converting every updated object row to jsonb.
CREATE TRIGGER update_objects_updated_at
BEFORE UPDATE ON storage.objects
FOR EACH ROW
WHEN (
    ROW(
        NEW.id,
        NEW.bucket_id,
        NEW.name,
        NEW.owner,
        NEW.created_at,
        NEW.updated_at,
        NEW.last_accessed_at,
        NEW.metadata,
        NEW.version,
        NEW.owner_id,
        NEW.user_metadata
    )
    IS DISTINCT FROM
    ROW(
        OLD.id,
        OLD.bucket_id,
        OLD.name,
        OLD.owner,
        OLD.created_at,
        OLD.updated_at,
        OLD.last_accessed_at,
        OLD.metadata,
        OLD.version,
        OLD.owner_id,
        OLD.user_metadata
    )
)
EXECUTE PROCEDURE update_updated_at_column();

CREATE OR REPLACE FUNCTION storage.enforce_objects_signature_client_writes()
    RETURNS trigger
AS $$
DECLARE
    anon_role text = COALESCE(current_setting('storage.anon_role', true), 'anon');
    authenticated_role text = COALESCE(current_setting('storage.authenticated_role', true), 'authenticated');
    effective_role text = COALESCE(NULLIF(current_setting('role', true), 'none'), current_user);
BEGIN
    IF effective_role = anon_role OR effective_role = authenticated_role THEN
        IF TG_OP = 'INSERT' AND NEW.signature IS NOT NULL THEN
            RAISE EXCEPTION 'Only storage service roles can set object signatures'
                USING ERRCODE = '42501';
        END IF;

        IF TG_OP = 'UPDATE'
            AND NEW.signature IS NOT NULL
            AND NEW.signature IS DISTINCT FROM OLD.signature
        THEN
            RAISE EXCEPTION 'Only storage service roles can set object signatures'
                USING ERRCODE = '42501';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS enforce_objects_signature_client_writes ON storage.objects;

CREATE TRIGGER enforce_objects_signature_client_writes
BEFORE INSERT OR UPDATE ON storage.objects
FOR EACH ROW
EXECUTE FUNCTION storage.enforce_objects_signature_client_writes();
