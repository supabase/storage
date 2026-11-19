-- postgres-migrations disable-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_multipart_uploads_parts_upload_part
ON storage.s3_multipart_uploads_parts (upload_id, part_number);
