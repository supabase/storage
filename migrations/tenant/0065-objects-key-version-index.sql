-- postgres-migrations disable-transaction

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS objects_bucket_id_name_version_key
    ON storage.objects (bucket_id, name, version) NULLS NOT DISTINCT;
