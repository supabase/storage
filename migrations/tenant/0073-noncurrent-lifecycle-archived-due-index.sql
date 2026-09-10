-- postgres-migrations disable-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS objects_archived_due_idx
ON storage.objects (
    bucket_id,
    archived_at,
    name COLLATE "C"
)
INCLUDE (version, is_delete_marker)
WHERE archived_at IS NOT NULL;
