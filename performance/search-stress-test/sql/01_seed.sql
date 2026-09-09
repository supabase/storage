-- Seeds two parallel datasets under a single bucket:
--   stress/            <folder_count> folders x <files_per_folder> files, no case
--                       collisions at all.
--   stress_collision/  the same <folder_count> folders, PLUS a case-flipped twin
--                       (FOLDER_xxxxxxx vs folder_xxxxxxx) for the first
--                       <collision_folder_count> of them. Twins get the exact same
--                       child file names as the original, which is deliberate: it's
--                       the worst case for the collision fix (identical relative
--                       names tie exactly on lower(name); see migrations/tenant/
--                       0063-fix-search-name-relative-to-prefix.sql for why that
--                       matters).
--
-- folder_count/files_per_folder are both derived from :total_rows so root-level
-- listing (many folders) and within-folder listing (many files) are both
-- meaningfully exercised regardless of scale.
--
-- Params (pass via -v name=value; all have defaults so this runs standalone):
--   total_rows        base row count for the clean dataset (default 1000)
--   collision_rate     fraction of folders that get a case-flipped twin (default 0.05)
--   bucket_id          test bucket id (default perf-stress-test)
--   batch_size         approx rows per INSERT/COMMIT batch (default 200000)
--   seed_clean         whether to (re)seed stress/ (default true)
--   seed_collision      whether to (re)seed stress_collision/ (default true)

\set ON_ERROR_STOP on
\i _defaults.sql

\if :{?total_rows}
\else
  \set total_rows 1000
\endif
\if :{?collision_rate}
\else
  \set collision_rate 0.05
\endif
\if :{?batch_size}
\else
  \set batch_size 200000
\endif
\if :{?seed_clean}
\else
  \set seed_clean true
\endif
\if :{?seed_collision}
\else
  \set seed_collision true
\endif

INSERT INTO storage.buckets (id, name)
VALUES (:'bucket_id', :'bucket_id')
ON CONFLICT (id) DO NOTHING;

-- folder_count grows with sqrt(total_rows), clamped to [10, 20000], so folder_count
-- and files_per_folder scale together instead of one dominating the other - both
-- the root listing (many folders) and within-folder listing (many files) benchmarks
-- need deep offsets to be meaningful, and that requires both dimensions to actually
-- grow with total_rows.
SELECT GREATEST(10, LEAST(20000, ROUND(SQRT(:total_rows))))::int AS folder_count
\gset

SELECT
  GREATEST(1, (:total_rows / :folder_count))::int AS files_per_folder,
  GREATEST(1, ROUND(:folder_count * :collision_rate))::int AS collision_folder_count
\gset

\echo Seeding plan: folder_count=:folder_count files_per_folder=:files_per_folder collision_folder_count=:collision_folder_count

\if :seed_clean
CALL perf.seed_folder_range(:'bucket_id', 'stress', 1, :folder_count, :files_per_folder, :batch_size, false, 'seed');

INSERT INTO perf.dataset_meta (bucket_id, scenario, folder_count, files_per_folder, collision_folder_count, total_rows)
VALUES (:'bucket_id', 'clean', :folder_count, :files_per_folder, 0, :folder_count * :files_per_folder)
ON CONFLICT (bucket_id, scenario) DO UPDATE SET
    folder_count = EXCLUDED.folder_count,
    files_per_folder = EXCLUDED.files_per_folder,
    collision_folder_count = EXCLUDED.collision_folder_count,
    total_rows = EXCLUDED.total_rows,
    seeded_at = clock_timestamp();
\endif

\if :seed_collision
CALL perf.seed_folder_range(:'bucket_id', 'stress_collision', 1, :folder_count, :files_per_folder, :batch_size, false, 'seedc');
CALL perf.seed_folder_range(:'bucket_id', 'stress_collision', 1, :collision_folder_count, :files_per_folder, :batch_size, true, 'seedt');

INSERT INTO perf.dataset_meta (bucket_id, scenario, folder_count, files_per_folder, collision_folder_count, total_rows)
VALUES (
    :'bucket_id', 'collision', :folder_count, :files_per_folder, :collision_folder_count,
    (:folder_count * :files_per_folder) + (:collision_folder_count * :files_per_folder)
)
ON CONFLICT (bucket_id, scenario) DO UPDATE SET
    folder_count = EXCLUDED.folder_count,
    files_per_folder = EXCLUDED.files_per_folder,
    collision_folder_count = EXCLUDED.collision_folder_count,
    total_rows = EXCLUDED.total_rows,
    seeded_at = clock_timestamp();
\endif

-- Bulk-loaded tables need a fresh ANALYZE - the planner's row estimates are what
-- decide index scan vs seq scan, and autovacuum may not have caught up yet at
-- large scale.
ANALYZE storage.objects;

\echo Seed complete.
SELECT * FROM perf.dataset_meta WHERE bucket_id = :'bucket_id' ORDER BY scenario;
