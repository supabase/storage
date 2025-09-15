DO $$
    DECLARE
        is_multitenant bool = COALESCE(current_setting('storage.multitenant', true), 'false')::boolean;
        anon_role text = COALESCE(current_setting('storage.anon_role', true), 'anon');
        authenticated_role text = COALESCE(current_setting('storage.authenticated_role', true), 'authenticated');
        service_role text = COALESCE(current_setting('storage.service_role', true), 'service_role');
    BEGIN
        CREATE TABLE IF NOT EXISTS storage.buckets_vectors (
            id text not null primary key,
            type storage.BucketType NOT NULL default 'VECTOR',
            created_at timestamptz NOT NULL default now(),
            updated_at timestamptz NOT NULL default now()
        );

        ALTER TABLE storage.buckets_vectors ENABLE ROW LEVEL SECURITY;

        EXECUTE 'GRANT ALL ON TABLE storage.buckets_vectors TO ' || service_role || ', ' || authenticated_role || ', ' || anon_role;

        IF is_multitenant THEN
            RETURN;
        END IF;

        CREATE TABLE IF NOT EXISTS storage.vector_indexes
        (
            id                     text COLLATE "C" primary key default gen_random_uuid(),
            bucket_id              text             NOT NULL references storage.buckets_vectors (id),
            data_type              text             NOT NULL,
            dimension              integer          NOT NULL,
            distance_metric        text             NOT NULL,
            metadata_configuration jsonb,
            created_at             timestamptz      NOT NULL default now(),
            updated_at             timestamptz      NOT NULL default now()
        );

        EXECUTE 'revoke all on storage.buckets_vectors from ' || anon_role || ', ' || authenticated_role;
        EXECUTE 'GRANT ALL ON TABLE storage.buckets_vectors TO ' || service_role;

        EXECUTE 'revoke all on storage.vector_indexes from ' || anon_role || ', ' || authenticated_role;
        EXECUTE 'GRANT ALL ON TABLE storage.vector_indexes TO ' || service_role;
        EXECUTE 'GRANT SELECT ON TABLE storage.vector_indexes TO ' || authenticated_role || ', ' || anon_role;
END$$;