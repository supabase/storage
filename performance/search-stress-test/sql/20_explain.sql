-- Runs EXPLAIN (ANALYZE, BUFFERS) against representative scenarios, both as a
-- black-box call to storage.search() (shows overall time/rows, but the function body
-- is opaque to the planner - it shows up as a single Function Scan node) and as the
-- individual internal queries the function actually executes at each step (shows
-- which index/scan each hot-path piece picks). Run once against a small dataset and
-- once against a large one; the planner can pick different strategies depending on
-- row counts, table statistics, and how the offset/limit interact with the chosen
-- plan, which is exactly what we're checking for here.
--
-- Requires 01_seed.sql to have been run first.
-- Params: bucket_id (see _defaults.sql).

\set ON_ERROR_STOP on
\i _defaults.sql

SELECT folder_count AS clean_folder_count, files_per_folder AS clean_files_per_folder
FROM perf.dataset_meta WHERE bucket_id = :'bucket_id' AND scenario = 'clean'
\gset

SELECT folder_count AS collision_folder_count
FROM perf.dataset_meta WHERE bucket_id = :'bucket_id' AND scenario = 'collision'
\gset

SELECT GREATEST(0, :clean_folder_count - 110)::int AS deep_offset \gset

-- The actual lower(name) boundary storage.search() would be seeking from at
-- deep_offset, so the standalone internal-query EXPLAINs below are representative
-- of a real deep-pagination call rather than an arbitrary offset=0 peek.
SELECT lower(name) AS deep_seek_bound
FROM storage.objects
WHERE bucket_id = :'bucket_id' AND name LIKE 'stress/%'
ORDER BY lower(name) COLLATE "C" ASC, name COLLATE "C" ASC
OFFSET :deep_offset LIMIT 1
\gset

\echo ================================================================
\echo 1) storage.search() black box - root listing, name asc, offset 0
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM storage.search('stress/', :'bucket_id', 100, 1, 0, '', 'name', 'asc');

\echo ================================================================
\echo 2) storage.search() black box - root listing, name asc, deep offset
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM storage.search('stress/', :'bucket_id', 100, 1, :deep_offset, '', 'name', 'asc');

\echo ================================================================
\echo 3) storage.search() black box - root listing, name desc, deep offset
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM storage.search('stress/', :'bucket_id', 100, 1, :deep_offset, '', 'name', 'desc');

\echo ================================================================
\echo 4) storage.search() black box - root listing, created_at asc, offset 0 (non-name path)
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM storage.search('stress/', :'bucket_id', 100, 1, 0, '', 'created_at', 'asc');

\echo ================================================================
\echo 5) storage.search() black box - root listing, created_at asc, deep offset (non-name path)
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM storage.search('stress/', :'bucket_id', 100, 1, :deep_offset, '', 'created_at', 'asc');

\echo ================================================================
\echo 6) storage.search() black box - root listing WITH case collisions, name asc, offset 0
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM storage.search('stress_collision/', :'bucket_id', 100, 1, 0, '', 'name', 'asc');

\echo ================================================================
\echo 7) storage.search() black box - within one large folder, name asc, deep offset
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT * FROM storage.search('stress/folder_0000001/', :'bucket_id', 100, 1,
    GREATEST(0, :clean_files_per_folder - 110), '', 'name', 'asc');

\echo ================================================================
\echo 8) internal peek query - the single-row lookup used for both folder detection
\echo    and file discovery in the main loop (ASC, bounded, deep seek position)
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT o.name FROM storage.objects o
WHERE o.bucket_id = :'bucket_id'
  AND lower(o.name) COLLATE "C" >= :'deep_seek_bound'
  AND lower(o.name) COLLATE "C" < 'stress0'
ORDER BY lower(o.name) COLLATE "C" ASC, o.name COLLATE "C" ASC LIMIT 1;

\echo ================================================================
\echo 9) internal batch query - the multi-row file fetch (ASC, bounded, deep seek)
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata
FROM storage.objects o
WHERE o.bucket_id = :'bucket_id'
  AND lower(o.name) COLLATE "C" >= :'deep_seek_bound'
  AND lower(o.name) COLLATE "C" < 'stress0'
ORDER BY lower(o.name) COLLATE "C" ASC, o.name COLLATE "C" ASC LIMIT 200;

\echo ================================================================
\echo 10) internal non-name-sort query - the ILIKE + GROUP BY path (folders half)
\echo     this is the query every non-name sortBy request runs, with no skip-scan
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT path_tokens[1] AS folder
FROM storage.objects
WHERE name ILIKE 'stress/' || '%'
  AND bucket_id = :'bucket_id'
  AND array_length(path_tokens, 1) <> 2
GROUP BY folder
ORDER BY folder ASC;

\echo ================================================================
\echo 11) internal collision existence check against an ACTUALLY-colliding folder -
\echo     this short-circuits on the first non-matching row, so it's cheap. See #12
\echo     for the far more common (and far more expensive) negative case.
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT EXISTS (
    SELECT 1 FROM storage.objects o
    WHERE o.bucket_id = :'bucket_id'
      AND lower(o.name) COLLATE "C" >= 'stress_collision/folder_0000001/'
      AND lower(o.name) COLLATE "C" < 'stress_collision/folder_00000010'
      AND left(o.name, length('stress_collision/folder_0000001/')) <> 'stress_collision/folder_0000001/'
);

\echo ================================================================
\echo 12) internal collision existence check against a NON-colliding folder - this is
\echo     the common case, and it cannot short-circuit: proving "no collision" means
\echo     scanning every row in the folder's case-insensitive range, unlike #11's
\echo     early exit. This is the one that drives the scaling cost in FINDINGS.md.
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT EXISTS (
    SELECT 1 FROM storage.objects o
    WHERE o.bucket_id = :'bucket_id'
      AND lower(o.name) COLLATE "C" >= 'stress/folder_0000001/'
      AND lower(o.name) COLLATE "C" < 'stress/folder_00000010'
      AND left(o.name, length('stress/folder_0000001/')) <> 'stress/folder_0000001/'
);

\echo ================================================================
\echo 13) internal collision resolution query - the array_agg/DISTINCT scan that
\echo     only runs once the existence check above finds a real collision
\echo ================================================================
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT array_agg(folder_exact ORDER BY folder_exact COLLATE "C" ASC)
FROM (
    SELECT DISTINCT storage.get_common_prefix(o.name, 'stress_collision/', '/') AS folder_exact
    FROM storage.objects o
    WHERE o.bucket_id = :'bucket_id'
      AND lower(o.name) COLLATE "C" >= 'stress_collision/folder_0000001/'
      AND lower(o.name) COLLATE "C" < 'stress_collision/folder_00000010'
      AND left(o.name, length('stress_collision/folder_0000001/')) <> 'stress_collision/folder_0000001/'
) t;

\echo Explain run complete.
