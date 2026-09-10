-- postgres-migrations disable-transaction

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS objects_archived_order_uq
ON storage.objects (bucket_id, name COLLATE "C", archived_at)
INCLUDE (version, is_delete_marker)
WHERE archived_at IS NOT NULL;
