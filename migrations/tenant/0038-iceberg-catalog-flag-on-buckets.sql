
DO $$
    DECLARE
        is_multitenant bool = COALESCE(current_setting('storage.multitenant', true), 'false')::boolean;
        anon_role text = COALESCE(current_setting('storage.anon_role', true), 'anon');
        authenticated_role text = COALESCE(current_setting('storage.authenticated_role', true), 'authenticated');
        service_role text = COALESCE(current_setting('storage.service_role', true), 'service_role');
    BEGIN
        ALTER TABLE storage.buckets
            ADD COLUMN IF NOT EXISTS iceberg_catalog boolean DEFAULT false NOT NULL;

        IF is_multitenant THEN
            RETURN;
        END IF;

        CREATE TABLE IF NOT EXISTS storage.iceberg_namespaces (
            id uuid primary key default gen_random_uuid(),
            bucket_id text NOT NULL references storage.buckets(id) ON DELETE CASCADE,
            name text COLLATE "C" NOT NULL,
            created_at timestamptz NOT NULL default now(),
            updated_at timestamptz NOT NULL default now()
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_iceberg_namespaces_bucket_id ON storage.iceberg_namespaces (bucket_id, name);

        CREATE TABLE IF NOT EXISTS storage.iceberg_tables (
          id uuid primary key default gen_random_uuid(),
          namespace_id uuid NOT NULL references storage.iceberg_namespaces(id) ON DELETE CASCADE,
          name text COLLATE "C" NOT NULL,
          location text not null,
          created_at timestamptz NOT NULL default now(),
          updated_at timestamptz NOT NULL default now()
        );

        CREATE UNIQUE INDEX idx_iceberg_tables_namespace_id ON storage.iceberg_tables (namespace_id, name);

        ALTER TABLE storage.iceberg_namespaces ENABLE ROW LEVEL SECURITY;
        ALTER TABLE storage.iceberg_tables ENABLE ROW LEVEL SECURITY;

        EXECUTE 'revoke all on storage.iceberg_namespaces from ' || anon_role || ', ' || authenticated_role;
        EXECUTE 'GRANT ALL ON TABLE storage.iceberg_namespaces TO ' || service_role;
        EXECUTE 'GRANT SELECT ON TABLE storage.iceberg_namespaces TO ' || authenticated_role || ', ' || anon_role;

        EXECUTE 'revoke all on storage.iceberg_tables from ' || anon_role || ', ' || authenticated_role;
        EXECUTE 'GRANT ALL ON TABLE storage.iceberg_tables TO ' || service_role;
        EXECUTE 'GRANT SELECT ON TABLE storage.iceberg_tables TO ' || authenticated_role || ', ' || anon_role;
END$$;