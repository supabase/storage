-- postgres-migrations disable-transaction

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_objects_current_version
    ON storage.objects (bucket_id, name) WHERE archived_at IS NULL;
