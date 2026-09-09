-- Benchmarks listing files inside a single large folder (stress/folder_0000001/,
-- from the clean dataset) across sort column x sort direction x offset depth. This
-- exercises the FILE batch-fetch loop rather than the folder skip-scan/collision
-- path exercised by 10_bench_root.sql. Requires 01_seed.sql to have been run first.
--
-- Params: run_id, function_label, bucket_id, bench_limit, sort_columns (see
-- _defaults.sql - pass e.g. -v sort_columns="ARRAY['name']" to skip the
-- non-name-sort path and run much faster at large scale).

\set ON_ERROR_STOP on
\i _defaults.sql

SELECT files_per_folder AS clean_files_per_folder
FROM perf.dataset_meta WHERE bucket_id = :'bucket_id' AND scenario = 'clean'
\gset

\echo Benchmarking within-folder listing for run_id=:run_id function_label=:function_label

SELECT perf.bench_offset_matrix(
    :'run_id', :'function_label', 'within-folder', 'stress/folder_0000001/', :'bucket_id',
    :bench_limit, :clean_files_per_folder, :clean_files_per_folder, 5, :sort_columns
);

\echo Within-folder benchmark complete.
SELECT scenario, sort_column, sort_order, offset_value, count(*) AS n,
       round(min(duration_ms), 2) AS min_ms, round(avg(duration_ms), 2) AS avg_ms, round(max(duration_ms), 2) AS max_ms
FROM perf.search_perf_results
WHERE run_id = :'run_id' AND function_label = :'function_label' AND scenario = 'within-folder'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;
