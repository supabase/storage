-- POST default-path dataset: same row count and key/folder layout as
-- seed-pre.sql, but on the full wave-2 schema, with every key single-version
-- (is_versioned = false, archived_at IS NULL, no delete markers) - i.e. a
-- bucket that has never touched versioning. Tests whether the rewritten
-- functions are slower than the pre-versioning ones for the common case, at
-- the same table scale and key/folder distribution as the PRE dataset.
-- Run against a DB migrated through wave-2's tip (no DB_MIGRATIONS_FREEZE_AT).
--
-- Override scale with: psql ... -v target_rows=100000 -f seed-post-default.sql
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
    created_at, updated_at, last_accessed_at,
    archived_at, is_delete_marker, is_versioned
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
    now() - (random() * interval '365 days'),
    NULL,
    false,
    false
FROM generate_series(0, :target_rows - 1) AS i;

-- A bulk load like this can race autovacuum's autoanalyze - benchmarking
-- against stale/missing statistics produces a wildly bad query plan that
-- looks like a catastrophic regression but is actually just this table
-- never having been analyzed yet. Always analyze explicitly before timing.
ANALYZE storage.objects;

SELECT count(*) AS total_rows, count(DISTINCT name) AS unique_keys,
       count(*) FILTER (WHERE archived_at IS NOT NULL) AS archived_rows
FROM storage.objects WHERE bucket_id = 'benchmark';
