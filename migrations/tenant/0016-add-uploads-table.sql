DO $$ BEGIN
    CREATE TYPE upload_type AS ENUM ('DIRECT', 'MULTIPART');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS uploads (
   id uuid NOT NULL DEFAULT extensions.uuid_generate_v4() PRIMARY KEY,
   bucket_id        text,
   object_name      text,
   owner            uuid,
   upload_type upload_type,
   expires_at timestamptz,
   created_at timestamptz DEFAULT now(),
   CONSTRAINT "uploads_bucketId_fkey" FOREIGN KEY ("bucket_id") REFERENCES "storage"."buckets"("id")
);

ALTER TABLE storage.uploads ENABLE ROW LEVEL SECURITY;