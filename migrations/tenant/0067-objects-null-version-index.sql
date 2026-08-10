-- postgres-migrations disable-transaction
-- Only one "null version" (is_versioned = false) row per key at a time
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_objects_null_version
    ON storage.objects (bucket_id, name) WHERE NOT is_versioned;
