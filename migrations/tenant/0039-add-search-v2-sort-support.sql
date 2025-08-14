CREATE OR REPLACE FUNCTION storage.search_v2 (
    prefix text,
    bucket_name text,
    limits int DEFAULT 100,
    levels int DEFAULT 1,
    start_after text DEFAULT '',
    sortcolumn text DEFAULT 'name',
    sortorder text DEFAULT 'asc'
) RETURNS TABLE (
    key text,
    name text,
    id uuid,
    updated_at timestamptz,
    created_at timestamptz,
    last_accessed_at timestamptz,
    metadata jsonb
)
SECURITY INVOKER
AS $func$
DECLARE
    sort_col text;
    sort_ord text;
    cursor_op text;
    cursor_expr text;
    collate_clause text := '';
BEGIN
    -- Validate sortorder
    sort_ord := lower(sortorder);
    IF sort_ord NOT IN ('asc', 'desc') THEN
        sort_ord := 'asc';
    END IF;

    -- Determine cursor comparison operator
    IF sort_ord = 'asc' THEN
        cursor_op := '>';
    ELSE
        cursor_op := '<';
    END IF;

    -- Validate sortcolumn
    sort_col := lower(sortcolumn);
    IF sort_col IN ('updated_at', 'created_at', 'last_accessed_at') THEN
        cursor_expr := format('($5 = '''' OR %I %s $5::timestamptz)', sort_col, cursor_op);
    ELSE
        sort_col := 'name';
        collate_clause := ' COLLATE "C"';
        cursor_expr := format('($5 = '''' OR %I%s %s $5)', sort_col, collate_clause, cursor_op);
    END IF;

    RETURN QUERY EXECUTE format(
        $sql$
        SELECT * FROM (
            (
                SELECT
                    split_part(name, '/', $4) AS key,
                    name || '/' AS name,
                    NULL::uuid AS id,
                    NULL::timestamptz AS updated_at,
                    NULL::timestamptz AS created_at,
                    NULL::timestamptz AS last_accessed_at,
                    NULL::jsonb AS metadata
                FROM storage.prefixes
                WHERE name COLLATE "C" LIKE $1 || '%%'
                    AND bucket_id = $2
                    AND level = $4
                    AND %s
                ORDER BY %I%s %s
                LIMIT $3
            )
            UNION ALL
            (
                SELECT
                    split_part(name, '/', $4) AS key,
                    name,
                    id,
                    updated_at,
                    created_at,
                    last_accessed_at,
                    metadata
                FROM storage.objects
                WHERE name COLLATE "C" LIKE $1 || '%%'
                    AND bucket_id = $2
                    AND level = $4
                    AND %s
                ORDER BY %I%s %s
                LIMIT $3
            )
        ) obj
        ORDER BY %I %s
        LIMIT $3
        $sql$,
        cursor_expr,         -- prefixes WHERE
        sort_col, collate_clause, sort_ord,  -- prefixes ORDER BY
        cursor_expr,         -- objects WHERE
        sort_col, collate_clause, sort_ord,  -- objects ORDER BY
        sort_col, collate_clause, sort_ord   -- final ORDER BY
    )
    USING prefix, bucket_name, limits, levels, start_after;
END;
$func$ LANGUAGE plpgsql STABLE;
