-- postgres-migrations disable-transaction
-- Only one "current" row per key at a time
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_objects_current_version
    ON storage.objects (bucket_id, name) WHERE archived_at IS NULL;
