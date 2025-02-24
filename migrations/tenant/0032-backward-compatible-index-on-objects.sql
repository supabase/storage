-- postgres-migrations disable-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_objects_lower_name ON storage.objects ((path_tokens[level]), lower(name) text_pattern_ops, bucket_id, level);
