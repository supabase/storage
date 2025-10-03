-- postgres-migrations disable-transaction

-- Helper function to calculate counts for all -1 prefixes
CREATE OR REPLACE FUNCTION storage.migrate_calculate_sentinel_prefixes()
RETURNS void
LANGUAGE plpgsql
AS $func$
DECLARE
    batch_size INTEGER := 10000;
    total_processed INTEGER := 0;
    rows_in_batch INTEGER := 0;
    delay INTEGER := 1;
    start_time TIMESTAMPTZ;
    exec_duration INTERVAL;
    BEGIN
        LOOP
            start_time := clock_timestamp();

            -- Batch of -1 prefixes to process
            WITH batch AS (
                SELECT bucket_id, name
                FROM storage.prefixes
                WHERE (child_objects < 0 OR child_prefixes < 0)
                ORDER BY bucket_id, name
                LIMIT batch_size
            ),
            -- Calculate child_objects for this batch
            object_counts AS (
                SELECT
                    b.bucket_id,
                    b.name,
                    COUNT(o.id) as child_count
                FROM batch b
                LEFT JOIN storage.objects o
                  ON o.bucket_id = b.bucket_id
                  AND regexp_replace(o.name, '/[^/]+$', '') = b.name
                  AND position('/' in o.name) > 0
                GROUP BY b.bucket_id, b.name
            ),
            -- Calculate child_prefixes for this batch
            prefix_counts AS (
                SELECT
                    b.bucket_id,
                    b.name,
                    COUNT(p.name) as child_count
                FROM batch b
                LEFT JOIN storage.prefixes p
                  ON p.bucket_id = b.bucket_id
                  AND regexp_replace(p.name, '/[^/]+$', '') = b.name
                  AND position('/' in p.name) > 0
                GROUP BY b.bucket_id, b.name
            )
            -- Update counts for this batch
            UPDATE storage.prefixes prf
            SET
                child_objects = COALESCE(oc.child_count, 0),
                child_prefixes = COALESCE(pc.child_count, 0)
            FROM batch b
            LEFT JOIN object_counts oc ON b.bucket_id = oc.bucket_id AND b.name = oc.name
            LEFT JOIN prefix_counts pc ON b.bucket_id = pc.bucket_id AND b.name = pc.name
            WHERE prf.bucket_id = b.bucket_id AND prf.name = b.name;

            GET DIAGNOSTICS rows_in_batch = ROW_COUNT;

            exec_duration := clock_timestamp() - start_time;
            total_processed := total_processed + COALESCE(rows_in_batch, 0);

            RAISE NOTICE '  Batch: % prefixes | Duration: % | Batch size: %',
                COALESCE(rows_in_batch, 0),
                exec_duration,
                batch_size;

            -- Exit when no more -1 prefixes remain
            EXIT WHEN rows_in_batch = 0;

            PERFORM pg_sleep(delay);

            -- Adaptive batch sizing (same as migration 0029)
            IF exec_duration > interval '3 seconds' THEN
                IF batch_size <= 20000 THEN
                    batch_size := GREATEST(batch_size - 1000, 5000);
                ELSE
                    batch_size := 20000;
                END IF;
            ELSE
                batch_size := LEAST(batch_size + 5000, 50000);
            END IF;

            delay := CASE WHEN delay >= 10 THEN 1 ELSE delay + 1 END;
        END LOOP;

        RAISE NOTICE '  Total prefixes processed: %', total_processed;

        -- Delete any orphaned prefixes (0,0 ref counts)
        DELETE FROM storage.prefixes
        WHERE child_objects = 0 AND child_prefixes = 0;

        GET DIAGNOSTICS rows_in_batch = ROW_COUNT;
        IF rows_in_batch > 0 THEN
            RAISE NOTICE '  Deleted % orphaned prefixes', rows_in_batch;
        END IF;
END;
$func$;

DO $$
DECLARE
    sentinel_count INTEGER := 0;
BEGIN
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Starting prefix ref counting migration';
    RAISE NOTICE 'Strategy: Process -1 sentinels with adaptive batching';
    RAISE NOTICE '=================================================';

    -- Create temporary index to speed up batch queries
    RAISE NOTICE 'Creating temporary index for migration...';
    CREATE INDEX IF NOT EXISTS prefixes_sentinel_idx
    ON storage.prefixes(bucket_id, name)
    WHERE (child_objects < 0 OR child_prefixes < 0);
    RAISE NOTICE 'Index created.';

    -- Phase 1: Calculate counts for all -1 prefixes
    RAISE NOTICE 'Phase 1: Calculating counts for all -1 prefixes...';

    PERFORM storage.migrate_calculate_sentinel_prefixes();

    -- Phase 2: Replace sentinel functions with real increment/decrement logic
    RAISE NOTICE 'Phase 2: Activating real increment/decrement functions...';

    -- Real increment_prefix_child_count (child_prefixes)
    CREATE OR REPLACE FUNCTION storage.increment_prefix_child_count(
        _bucket_id text,
        _child_name text
    )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
    AS $func$
    DECLARE
        _parent_name text;
    BEGIN
        _parent_name := storage.get_direct_parent(_child_name);

        IF _parent_name = '' THEN
            RETURN;
        END IF;

        PERFORM storage.lock_prefix(_bucket_id, _parent_name);

        INSERT INTO storage.prefixes (bucket_id, name, child_objects, child_prefixes)
        VALUES (_bucket_id, _parent_name, 0, 1)
        ON CONFLICT (bucket_id, level, name)
        DO UPDATE SET child_prefixes = storage.prefixes.child_prefixes + 1;
    END;
    $func$;

    -- Real decrement_prefix_child_count (child_prefixes)
    CREATE OR REPLACE FUNCTION storage.decrement_prefix_child_count(
        _bucket_id text,
        _child_name text
    )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
    AS $func$
    DECLARE
        _parent_name text;
        _new_object_count integer;
        _new_prefix_count integer;
    BEGIN
        _parent_name := storage.get_direct_parent(_child_name);

        IF _parent_name = '' THEN
            RETURN;
        END IF;

        PERFORM storage.lock_prefix(_bucket_id, _parent_name);

        UPDATE storage.prefixes
        SET child_prefixes = child_prefixes - 1
        WHERE storage.prefixes.bucket_id = _bucket_id
          AND storage.prefixes.name = _parent_name
        RETURNING child_objects, child_prefixes
        INTO _new_object_count, _new_prefix_count;

        IF _new_object_count = 0 AND _new_prefix_count = 0 THEN
            DELETE FROM storage.prefixes
            WHERE storage.prefixes.bucket_id = _bucket_id
              AND storage.prefixes.name = _parent_name
              AND child_objects = 0
              AND child_prefixes = 0;
        END IF;
    END;
    $func$;

    -- Real increment_prefix_object_count (child_objects)
    CREATE OR REPLACE FUNCTION storage.increment_prefix_object_count(
        _bucket_id text,
        _prefix_name text,
        _count bigint
    )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
    AS $func$
    BEGIN
        IF _prefix_name = '' THEN
            RETURN;
        END IF;

        INSERT INTO storage.prefixes (bucket_id, name, child_objects, child_prefixes)
        VALUES (_bucket_id, _prefix_name, _count, 0)
        ON CONFLICT (bucket_id, level, name)
        DO UPDATE SET child_objects = storage.prefixes.child_objects + _count;
    END;
    $func$;

    -- Real decrement_prefix_object_count (child_objects)
    CREATE OR REPLACE FUNCTION storage.decrement_prefix_object_count(
        _bucket_id text,
        _prefix_name text,
        _count bigint
    )
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
    AS $func$
    DECLARE
        _new_object_count integer;
        _new_prefix_count integer;
    BEGIN
        IF _prefix_name = '' THEN
            RETURN;
        END IF;

        UPDATE storage.prefixes
        SET child_objects = child_objects - _count
        WHERE storage.prefixes.bucket_id = _bucket_id
          AND storage.prefixes.name = _prefix_name
        RETURNING child_objects, child_prefixes
        INTO _new_object_count, _new_prefix_count;

        IF _new_object_count = 0 AND _new_prefix_count = 0 THEN
            DELETE FROM storage.prefixes
            WHERE storage.prefixes.bucket_id = _bucket_id
              AND storage.prefixes.name = _prefix_name
              AND child_objects = 0
              AND child_prefixes = 0;
        END IF;
    END;
    $func$;

    RAISE NOTICE '  Replaced 4 functions with real increment/decrement logic';

    -- Phase 3: Final validation and cleanup of any -1 prefixes created during migration
    RAISE NOTICE 'Phase 3: Checking for -1 prefixes created during migration...';

    SELECT COUNT(*) INTO sentinel_count
    FROM storage.prefixes
    WHERE child_objects < 0 OR child_prefixes < 0;

    IF sentinel_count > 0 THEN
        RAISE NOTICE '  Found % -1 prefixes created during migration. Calculating final counts...', sentinel_count;
        PERFORM storage.migrate_calculate_sentinel_prefixes();
    ELSE
        RAISE NOTICE '  No -1 prefixes found - migration was clean!';
    END IF;

    -- Drop temporary index
    RAISE NOTICE 'Dropping temporary index...';
    DROP INDEX IF EXISTS storage.prefixes_sentinel_idx;
    RAISE NOTICE 'Index dropped.';

    -- Final summary
    RAISE NOTICE '=================================================';
    RAISE NOTICE 'Migration complete!';
    RAISE NOTICE 'All counts are now accurate and maintained by triggers.';
    RAISE NOTICE '=================================================';
END;
$$;

-- Drop the helper function (no longer needed after migration)
DROP FUNCTION IF EXISTS storage.migrate_calculate_sentinel_prefixes();
