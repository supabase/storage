-- PRE baseline dataset: flat rows, one per key, matching the schema as it
-- existed before object-versioning-core (no archived_at/is_delete_marker/
-- is_versioned columns). Run only against a DB migrated up to
-- 'mark-filename-immutable' via DB_MIGRATIONS_FREEZE_AT - see benchmark/README.md.
--
-- Override scale with: psql ... -v target_rows=100000 -f seed-pre.sql
\if :{?target_rows}
\else
\set target_rows 10000000
\endif
\if :{?keys_per_folder}
\else
\set keys_per_folder 1000
\endif

INSERT INTO storage.buckets (id, name, public)
VALUES ('benchmark', 'benchmark', false)
ON CONFLICT (id) DO NOTHING;

INSERT INTO storage.objects (
    bucket_id, name, owner, owner_id, version, metadata,
    created_at, updated_at, last_accessed_at
)
SELECT
    'benchmark',
    'folder-' || lpad((i / :keys_per_folder)::text, 5, '0')
        || '/key-' || lpad((i % :keys_per_folder)::text, 6, '0') || '.bin',
    NULL,
    NULL,
    gen_random_uuid()::text,
    jsonb_build_object('size', 1024, 'mimetype', 'application/octet-stream'),
    now() - (random() * interval '365 days'),
    now() - (random() * interval '365 days'),
    now() - (random() * interval '365 days')
FROM generate_series(0, :target_rows - 1) AS i;

-- A bulk load like this can race autovacuum's autoanalyze - benchmarking
-- against stale/missing statistics produces a wildly bad query plan that
-- looks like a catastrophic regression but is actually just this table
-- never having been analyzed yet. Always analyze explicitly before timing.
ANALYZE storage.objects;

SELECT count(*) AS total_rows, count(DISTINCT name) AS unique_keys FROM storage.objects WHERE bucket_id = 'benchmark';
