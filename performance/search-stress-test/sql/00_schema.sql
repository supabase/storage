-- Creates the bookkeeping objects used by this harness. Idempotent - safe to re-run.
-- Lives in its own schema so it never collides with anything storage/tenant related,
-- and can be dropped in one shot with `DROP SCHEMA perf CASCADE;`.

CREATE SCHEMA IF NOT EXISTS perf;

-- One row per timed storage.search() call.
CREATE TABLE IF NOT EXISTS perf.search_perf_results (
    id                bigserial PRIMARY KEY,
    run_id            text NOT NULL,
    function_label    text NOT NULL,
    scenario          text NOT NULL,
    prefix_used       text NOT NULL,
    sort_column       text NOT NULL,
    sort_order        text NOT NULL,
    offset_value      bigint NOT NULL,
    limit_value       int NOT NULL,
    total_rows        bigint NOT NULL,
    rows_returned     int NOT NULL,
    duration_ms       numeric NOT NULL,
    executed_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS search_perf_results_run_id_idx ON perf.search_perf_results (run_id);

-- Records the shape of a seeded dataset so bench/explain scripts can derive
-- meaningful offsets (shallow/mid/deep) without re-deriving folder counts by hand.
CREATE TABLE IF NOT EXISTS perf.dataset_meta (
    bucket_id               text NOT NULL,
    scenario                text NOT NULL, -- 'clean' | 'collision'
    folder_count             bigint NOT NULL,
    files_per_folder         bigint NOT NULL,
    collision_folder_count   bigint NOT NULL DEFAULT 0,
    total_rows               bigint NOT NULL,
    seeded_at                timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bucket_id, scenario)
);

-- Times a single storage.search() call and logs it. Wrapping the timing in SQL
-- (clock_timestamp(), not psql's \timing) keeps the result independent of how the
-- caller invokes it and avoids parsing psql's text output.
CREATE OR REPLACE FUNCTION perf.time_search_call(
    p_run_id         text,
    p_function_label text,
    p_scenario       text,
    p_prefix         text,
    p_bucket         text,
    p_limit          int,
    p_offset         bigint,
    p_sort_column    text,
    p_sort_order     text,
    p_total_rows     bigint
) RETURNS numeric
LANGUAGE plpgsql
AS $$
DECLARE
    v_start     timestamptz;
    v_duration  numeric;
    v_count     int;
BEGIN
    v_start := clock_timestamp();

    SELECT count(*) INTO v_count
    FROM storage.search(p_prefix, p_bucket, p_limit, 1, p_offset::int, '', p_sort_column, p_sort_order);

    v_duration := extract(epoch FROM (clock_timestamp() - v_start)) * 1000;

    INSERT INTO perf.search_perf_results (
        run_id, function_label, scenario, prefix_used, sort_column, sort_order,
        offset_value, limit_value, total_rows, rows_returned, duration_ms
    ) VALUES (
        p_run_id, p_function_label, p_scenario, p_prefix, p_sort_column, p_sort_order,
        p_offset, p_limit, p_total_rows, v_count, v_duration
    );

    RETURN v_duration;
END;
$$;

-- Inserts folders [p_start, p_end] x p_files_per_folder files each, batching in
-- chunks of roughly p_batch_size rows with a COMMIT between chunks (so a 100M-row
-- seed doesn't sit in one giant transaction, and progress is visible via NOTICEs).
-- A PROCEDURE (not a plain function/DO block) so it can COMMIT internally - see
-- https://www.postgresql.org/docs/current/plpgsql-transactions.html. All values
-- come in as real parameters rather than psql :variables, since psql does not
-- substitute :variables inside $$-quoted bodies.
CREATE OR REPLACE PROCEDURE perf.seed_folder_range(
    p_bucket_id         text,
    p_prefix             text,    -- e.g. 'stress' or 'stress_collision', no trailing slash
    p_start              int,
    p_end                int,
    p_files_per_folder   int,
    p_batch_size         int,
    p_upper_case_folder  boolean DEFAULT false,
    p_version_tag        text DEFAULT 'seed'
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_folders_per_batch int;
    v_batch_start        int;
    v_batch_end          int;
    v_folder_token        text;
BEGIN
    v_folders_per_batch := GREATEST(1, p_batch_size / GREATEST(1, p_files_per_folder));
    v_folder_token := CASE WHEN p_upper_case_folder THEN 'FOLDER_' ELSE 'folder_' END;
    v_batch_start := p_start;

    WHILE v_batch_start <= p_end LOOP
        v_batch_end := LEAST(p_end, v_batch_start + v_folders_per_batch - 1);

        INSERT INTO storage.objects (bucket_id, name, owner, owner_id, version, metadata)
        SELECT p_bucket_id,
               p_prefix || '/' || v_folder_token || lpad(f::text, 7, '0') || '/file_' || lpad(i::text, 6, '0') || '.dat',
               '00000000-0000-0000-0000-000000000000'::uuid,
               '00000000-0000-0000-0000-000000000000',
               p_version_tag || '-' || f || '-' || i,
               jsonb_build_object('size', 1234)
        FROM generate_series(v_batch_start, v_batch_end) f
        CROSS JOIN generate_series(1, p_files_per_folder) i;

        RAISE NOTICE '%/%: folders % of % seeded', p_prefix, v_folder_token, v_batch_end, p_end;
        COMMIT;

        v_batch_start := v_batch_end + 1;
    END LOOP;
END;
$$;

-- Times storage.search() across a small offset/sort matrix (both sort directions,
-- name + one non-name column, at a shallow/mid/deep offset) for one scenario, and
-- logs every call via time_search_call. p_total_items is whatever the offset tiers
-- should be computed relative to (folder_count for a root listing, files_per_folder
-- for a within-folder listing).
--
-- Each combination is called once, untimed, to warm caches/plans, then p_repeats
-- times, timed and logged - individual call latency is noisy enough (cold buffer
-- pages, autovacuum, concurrent checkpoints) that a single sample per combination
-- produced results that didn't reproduce on a repeat run; average over p_repeats
-- instead of trusting any one sample.
CREATE OR REPLACE FUNCTION perf.bench_offset_matrix(
    p_run_id         text,
    p_function_label text,
    p_scenario       text,
    p_prefix         text,
    p_bucket_id      text,
    p_limit          int,
    p_total_items    bigint,
    p_total_rows     bigint,
    p_repeats        int DEFAULT 5,
    p_sort_columns   text[] DEFAULT ARRAY['name', 'created_at']
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_sort_columns text[] := p_sort_columns;
    v_sort_orders  text[] := ARRAY['asc', 'desc'];
    v_offsets      bigint[];
    v_col          text;
    v_ord          text;
    v_off          bigint;
    v_i            int;
BEGIN
    v_offsets := ARRAY[
        0::bigint,
        GREATEST(0, (p_total_items / 2) - p_limit),
        GREATEST(0, p_total_items - p_limit - 10)
    ];

    FOREACH v_col IN ARRAY v_sort_columns LOOP
        FOREACH v_ord IN ARRAY v_sort_orders LOOP
            FOREACH v_off IN ARRAY v_offsets LOOP
                PERFORM count(*) FROM storage.search(
                    p_prefix, p_bucket_id, p_limit, 1, v_off::int, '', v_col, v_ord
                );

                FOR v_i IN 1..p_repeats LOOP
                    PERFORM perf.time_search_call(
                        p_run_id, p_function_label, p_scenario, p_prefix, p_bucket_id,
                        p_limit, v_off, v_col, v_ord, p_total_rows
                    );
                END LOOP;
            END LOOP;
        END LOOP;
    END LOOP;
END;
$$;
