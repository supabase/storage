

DO $$ BEGIN
    CREATE TYPE upload_state AS ENUM ('STARTED', 'COMPLETED');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

alter table storage.objects add column if not exists upload_state upload_state default null;
