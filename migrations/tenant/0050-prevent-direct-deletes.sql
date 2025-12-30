-- Create a function that prevents direct deletes unless storage.can_delete is set
CREATE OR REPLACE FUNCTION storage.protect_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check if storage.can_delete is set to 'true'
    IF COALESCE(current_setting('storage.can_delete', true), 'false') != 'true' THEN
        RAISE EXCEPTION 'Direct deletion from storage tables is not allowed. Use the Storage API instead.'
            USING HINT = 'This prevents accidental data loss from orphaned objects.',
                  ERRCODE = '42501';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER protect_buckets_delete
    BEFORE DELETE ON storage.buckets
    FOR EACH STATEMENT
    EXECUTE FUNCTION storage.protect_delete();

CREATE TRIGGER protect_objects_delete
    BEFORE DELETE ON storage.objects
    FOR EACH STATEMENT
    EXECUTE FUNCTION storage.protect_delete();

CREATE TRIGGER protect_s3_multipart_uploads_delete
    BEFORE DELETE ON storage.s3_multipart_uploads
    FOR EACH STATEMENT
    EXECUTE FUNCTION storage.protect_delete();

CREATE TRIGGER protect_s3_multipart_uploads_parts_delete
    BEFORE DELETE ON storage.s3_multipart_uploads_parts
    FOR EACH STATEMENT
    EXECUTE FUNCTION storage.protect_delete();

CREATE TRIGGER protect_buckets_analytics_delete
    BEFORE DELETE ON storage.buckets_analytics
    FOR EACH STATEMENT
    EXECUTE FUNCTION storage.protect_delete();

CREATE TRIGGER protect_buckets_vectors_delete
    BEFORE DELETE ON storage.buckets_vectors
    FOR EACH STATEMENT
    EXECUTE FUNCTION storage.protect_delete();

CREATE TRIGGER protect_vector_indexes_delete
    BEFORE DELETE ON storage.vector_indexes
    FOR EACH STATEMENT
    EXECUTE FUNCTION storage.protect_delete();

-- Create triggers for iceberg tables (only exist in non-multitenant mode)
DO $$
    DECLARE
        is_multitenant bool = COALESCE(current_setting('storage.multitenant', true), 'false')::boolean;
    BEGIN
        IF is_multitenant THEN
            RETURN;
        END IF;

        CREATE TRIGGER protect_iceberg_namespaces_delete
            BEFORE DELETE ON storage.iceberg_namespaces
            FOR EACH STATEMENT
            EXECUTE FUNCTION storage.protect_delete();

        CREATE TRIGGER protect_iceberg_tables_delete
            BEFORE DELETE ON storage.iceberg_tables
            FOR EACH STATEMENT
            EXECUTE FUNCTION storage.protect_delete();
END$$;
