ALTER TABLE storage.objects ADD COLUMN user_metadata jsonb NULL;
ALTER TABLE storage.s3_multipart_uploads ADD COLUMN user_metadata jsonb NULL;