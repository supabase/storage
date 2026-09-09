INSERT INTO storage.buckets (id, name, public)
VALUES ('benchmark-current-version-peek', 'benchmark-current-version-peek', false)
ON CONFLICT (id) DO NOTHING;

WITH keys AS (
    SELECT folder_idx,
           'folder-' || lpad(folder_idx::text, 5, '0') || '/object.bin' AS name
    FROM generate_series(0, 999) AS folder_idx
), versions AS (
    SELECT version_idx
    FROM generate_series(1, 1000) AS version_idx
)
INSERT INTO storage.objects (
    bucket_id, name, version, archived_at, is_versioned,
    is_delete_marker, metadata
)
SELECT
    'benchmark-current-version-peek',
    keys.name,
    'version-' || lpad(versions.version_idx::text, 4, '0'),
    CASE WHEN versions.version_idx = 1000 THEN NULL ELSE now() - versions.version_idx * interval '1 minute' END,
    true,
    false,
    '{"size": 1024, "mimetype": "application/octet-stream"}'::jsonb
FROM keys
CROSS JOIN versions;

ANALYZE storage.objects;
