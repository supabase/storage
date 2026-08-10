-- postgres-migrations disable-transaction
-- For lifecycle/expiration queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_objects_bucket_id_archived_at
    ON storage.objects (bucket_id, archived_at);
