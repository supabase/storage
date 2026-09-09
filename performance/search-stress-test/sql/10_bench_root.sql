-- Benchmarks root-level folder listing (many sibling folders under stress/ and
-- stress_collision/) across sort column x sort direction x offset depth, for both
-- the clean and collision datasets. Requires 01_seed.sql to have been run first.
--
-- Params: run_id, function_label, bucket_id, bench_limit, sort_columns (see
-- _defaults.sql - pass e.g. -v sort_columns="ARRAY['name']" to skip the
-- non-name-sort path and run much faster at large scale).

\set ON_ERROR_STOP on
\i _defaults.sql

SELECT folder_count AS clean_folder_count, total_rows AS clean_total_rows
FROM perf.dataset_meta WHERE bucket_id = :'bucket_id' AND scenario = 'clean'
\gset

SELECT folder_count AS collision_folder_count, total_rows AS collision_total_rows
FROM perf.dataset_meta WHERE bucket_id = :'bucket_id' AND scenario = 'collision'
\gset

\echo Benchmarking root listing for run_id=:run_id function_label=:function_label

SELECT perf.bench_offset_matrix(
    :'run_id', :'function_label', 'root-clean', 'stress/', :'bucket_id',
    :bench_limit, :clean_folder_count, :clean_total_rows, 5, :sort_columns
);

SELECT perf.bench_offset_matrix(
    :'run_id', :'function_label', 'root-collision', 'stress_collision/', :'bucket_id',
    :bench_limit, :collision_folder_count, :collision_total_rows, 5, :sort_columns
);

\echo Root listing benchmark complete.
SELECT scenario, sort_column, sort_order, offset_value, count(*) AS n,
       round(min(duration_ms), 2) AS min_ms, round(avg(duration_ms), 2) AS avg_ms, round(max(duration_ms), 2) AS max_ms
FROM perf.search_perf_results
WHERE run_id = :'run_id' AND function_label = :'function_label' AND scenario LIKE 'root-%'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;
