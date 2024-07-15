CREATE TABLE storage.disks (
        "id" uuid NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
        name text NOT NULL unique,
        mount_point text NOT NULL,
        credentials jsonb NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE storage.buckets ADD COLUMN disk_id uuid DEFAULT NULL;
ALTER TABLE storage.buckets ADD CONSTRAINT fk_disk FOREIGN KEY (disk_id) REFERENCES storage.disks(id);

ALTER TABLE storage.disks ENABLE ROW LEVEL SECURITY;

DO $$
    DECLARE
        anon_role text = COALESCE(current_setting('storage.anon_role', true), 'anon');
        authenticated_role text = COALESCE(current_setting('storage.authenticated_role', true), 'authenticated');
    BEGIN
        EXECUTE 'revoke all on storage.disks from ' || anon_role || ', ' || authenticated_role;
    END$$;