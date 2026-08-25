-- POST heavy-version dataset: realistic long-tail version distribution over
-- ~:target_rows total rows, on the full wave-2 schema. Three buckets of keys:
--   ~90.0% of keys: 1 version (current only)
--   ~9.5%  of keys: 2-5 versions (modest overwrite history)
--   ~0.5%  of keys: 20-50 versions (deep history / hot keys), ~10% of which
--                   have their current row replaced by a delete marker
--
-- Note: bucket.versioning_status stays at the forced 'DISABLED' default (this
-- branch is off wave-2, before the wave-3 unlock migration exists) - this has
-- no bearing on the benchmark, since wave-2's SQL functions take
-- noncurrent_versions/delete_markers as explicit call parameters and never
-- consult bucket.versioning_status at all.
--
-- Run against a DB migrated through wave-2's tip (no DB_MIGRATIONS_FREEZE_AT).
-- Run against a table already TRUNCATEd if reusing a DB from seed-post-default.sql.
--
-- Override scale with: psql ... -v target_rows=100000 -f seed-post-versioned.sql
\if :{?target_rows}
\else
\set target_rows 10000000
\endif
\if :{?keys_per_folder}
\else
\set keys_per_folder 1000
\endif
-- Weighted average versions-per-key, used to size the key pool so total rows
-- land close to :target_rows: 0.90*1 + 0.095*3.5 + 0.005*30 = 1.3825
\set avg_versions_per_key 1.3825

SELECT (:target_rows / :avg_versions_per_key)::bigint AS total_keys \gset

INSERT INTO storage.buckets (id, name, public)
VALUES ('benchmark', 'benchmark', false)
ON CONFLICT (id) DO NOTHING;

WITH bucketed AS (
    SELECT
        i AS key_idx,
        random() AS delete_marker_roll,
        (i >= :total_keys * 0.995) AS is_deep_bucket,
        CASE
            WHEN i < :total_keys * 0.90 THEN 1
            WHEN i < :total_keys * 0.995 THEN 2 + floor(random() * 4)::int  -- 2-5
            ELSE 20 + floor(random() * 31)::int                            -- 20-50
        END AS version_count
    FROM generate_series(0, :total_keys - 1) AS i
)
INSERT INTO storage.objects (
    bucket_id, name, owner, owner_id, version, metadata, user_metadata,
    created_at, updated_at, last_accessed_at,
    archived_at, is_delete_marker, is_versioned
)
SELECT
    'benchmark',
    'folder-' || lpad((b.key_idx / :keys_per_folder)::text, 5, '0')
        || '/key-' || lpad((b.key_idx % :keys_per_folder)::text, 6, '0') || '.bin',
    NULL,
    NULL,
    gen_random_uuid()::text,
    CASE
        WHEN v.version_num = b.version_count AND b.is_deep_bucket AND b.delete_marker_roll < 0.10
            THEN NULL
        ELSE jsonb_build_object('size', 1024, 'mimetype', 'application/octet-stream')
    END,
    NULL,
    now() - ((b.version_count - v.version_num + 1) * interval '1 day') - (random() * interval '1 day'),
    now() - ((b.version_count - v.version_num + 1) * interval '1 day') - (random() * interval '1 day'),
    now() - (random() * interval '365 days'),
    CASE
        WHEN v.version_num = b.version_count THEN NULL
        ELSE now() - ((b.version_count - v.version_num) * interval '1 day')
    END,
    (v.version_num = b.version_count AND b.is_deep_bucket AND b.delete_marker_roll < 0.10),
    true
FROM bucketed b
CROSS JOIN LATERAL generate_series(1, b.version_count) AS v(version_num);

-- A bulk load like this can race autovacuum's autoanalyze - benchmarking
-- against stale/missing statistics produces a wildly bad query plan that
-- looks like a catastrophic regression but is actually just this table
-- never having been analyzed yet. Always analyze explicitly before timing.
ANALYZE storage.objects;

SELECT count(*) AS total_rows, count(DISTINCT name) AS unique_keys,
       count(*) FILTER (WHERE archived_at IS NOT NULL) AS archived_rows,
       count(*) FILTER (WHERE is_delete_marker) AS delete_marker_rows
FROM storage.objects WHERE bucket_id = 'benchmark';
