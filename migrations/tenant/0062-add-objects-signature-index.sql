-- postgres-migrations disable-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_objects_missing_signature_bucket_id_name
    ON storage.objects (bucket_id, name)
    INCLUDE (version)
    WHERE signature IS NULL;
