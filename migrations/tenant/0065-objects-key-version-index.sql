-- postgres-migrations disable-transaction
-- Row identity for versioned objects: at most one row per (bucket_id, name, version)
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS objects_bucket_id_name_version_key
    ON storage.objects (bucket_id, name, version) NULLS NOT DISTINCT;
