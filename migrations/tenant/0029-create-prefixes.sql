-- postgres-migrations disable-transaction
-- Backfill prefixes table records
-- We run this with 50k batch size to avoid long running transaction
DO $$
    DECLARE
        batch_size INTEGER := 50000;
        total_scanned INTEGER := 0;
        row_returned INTEGER := 0;
        last_name TEXT COLLATE "C" := NULL;
        last_bucket_id TEXT COLLATE "C" := NULL;
    BEGIN
        LOOP
            -- Fetch a batch of objects ordered by name COLLATE "C"
            WITH batch as (
                SELECT id, bucket_id, name, owner
                FROM storage.objects
                WHERE (last_name IS NULL OR ((name COLLATE "C", bucket_id) > (last_name, last_bucket_id)))
                ORDER BY name COLLATE "C", bucket_id
                LIMIT batch_size
            ),
            batch_count as (
                SELECT COUNT(*) as count FROM batch
            ),
            cursor as (
                 SELECT name as last_name, bucket_id as last_bucket FROM batch b
                 ORDER BY name COLLATE "C" DESC, bucket_id DESC LIMIT 1
            ),
            all_prefixes as (
                SELECT UNNEST(storage.get_prefixes(name)) as prefix, bucket_id
                FROM batch
            ),
            insert_prefixes as (
                INSERT INTO storage.prefixes (bucket_id, name)
                SELECT bucket_id, prefix FROM all_prefixes
                WHERE coalesce(prefix, '') != ''
                ON CONFLICT DO NOTHING
            )
            SELECT count, cursor.last_name, cursor.last_bucket FROM cursor, batch_count INTO row_returned, last_name, last_bucket_id;

            RAISE NOTICE 'Object Row returned: %', row_returned;
            RAISE NOTICE 'Last Object: %', last_name;

            total_scanned := total_scanned + row_returned;

            IF row_returned IS NULL OR row_returned < batch_size THEN
                RAISE NOTICE 'Total Object scanned: %', coalesce(total_scanned, 0);
                COMMIT;
                EXIT;
            ELSE
                COMMIT;
            END IF;
    END LOOP;
END;
$$;