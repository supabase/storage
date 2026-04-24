CREATE OR REPLACE FUNCTION storage.get_common_prefix(
    p_key TEXT,
    p_prefix TEXT,
    p_delimiter TEXT
) RETURNS TEXT
IMMUTABLE
LANGUAGE plpgsql
AS $$
DECLARE
    v_prefix TEXT := coalesce(p_prefix, '');
    v_suffix TEXT;
    v_scan_index INT := 1;
    v_next_delimiter_index INT;
BEGIN
    IF coalesce(p_delimiter, '') = '' THEN
        RETURN NULL;
    END IF;

    IF v_prefix <> '' AND lower(left(p_key, length(v_prefix))) <> lower(v_prefix) THEN
        RETURN NULL;
    END IF;

    v_suffix := substring(p_key FROM length(v_prefix) + 1);

    -- Only skip leading delimiters when the prefix already ends with the
    -- delimiter (empty-segment case like 'prefix//file'). Otherwise the
    -- first delimiter in the suffix is the real folder boundary.
    IF right(v_prefix, length(p_delimiter)) = p_delimiter THEN
        WHILE left(substring(v_suffix FROM v_scan_index), length(p_delimiter)) = p_delimiter LOOP
            v_scan_index := v_scan_index + length(p_delimiter);
        END LOOP;
    END IF;

    v_next_delimiter_index := position(p_delimiter IN substring(v_suffix FROM v_scan_index));
    IF v_next_delimiter_index = 0 THEN
        RETURN NULL;
    END IF;

    RETURN left(
        p_key,
        length(v_prefix) + (v_scan_index - 1) + v_next_delimiter_index - 1 + length(p_delimiter)
    );
END;
$$;

CREATE OR REPLACE FUNCTION storage.get_prefix_child_name(
    p_key TEXT,
    p_prefix TEXT,
    p_delimiter TEXT
) RETURNS TEXT
IMMUTABLE
LANGUAGE plpgsql
AS $$
DECLARE
    v_prefix TEXT := coalesce(p_prefix, '');
    v_suffix TEXT;
    v_scan_index INT := 1;
    v_trimmed_suffix TEXT;
BEGIN
    IF coalesce(p_delimiter, '') = '' THEN
        RETURN NULL;
    END IF;

    IF v_prefix <> '' AND lower(left(p_key, length(v_prefix))) <> lower(v_prefix) THEN
        RETURN NULL;
    END IF;

    v_suffix := substring(p_key FROM length(v_prefix) + 1);

    IF right(v_prefix, length(p_delimiter)) = p_delimiter THEN
        WHILE left(substring(v_suffix FROM v_scan_index), length(p_delimiter)) = p_delimiter LOOP
            v_scan_index := v_scan_index + length(p_delimiter);
        END LOOP;
    END IF;

    v_trimmed_suffix := substring(v_suffix FROM v_scan_index);
    IF coalesce(v_trimmed_suffix, '') = '' THEN
        RETURN NULL;
    END IF;

    RETURN split_part(v_trimmed_suffix, p_delimiter, 1);
END;
$$;

CREATE OR REPLACE FUNCTION storage.search(
    prefix text,
    bucketname text,
    limits int DEFAULT 100,
    levels int DEFAULT 1,
    offsets int DEFAULT 0,
    search text DEFAULT '',
    sortcolumn text DEFAULT 'name',
    sortorder text DEFAULT 'asc'
)
RETURNS TABLE (
    name text,
    id uuid,
    updated_at timestamptz,
    created_at timestamptz,
    last_accessed_at timestamptz,
    metadata jsonb
)
SECURITY INVOKER
LANGUAGE plpgsql STABLE
AS $func$
DECLARE
    v_peek_name TEXT;
    v_current RECORD;
    v_common_prefix TEXT;
    v_child_name TEXT;
    v_delimiter CONSTANT TEXT := '/';

    -- Configuration
    v_limit INT;
    v_prefix TEXT;
    v_raw_prefix TEXT;
    v_prefix_lower TEXT;
    v_is_asc BOOLEAN;
    v_order_by TEXT;
    v_sort_order TEXT;
    v_upper_bound TEXT;
    v_file_batch_size INT;

    -- Dynamic SQL for batch query only
    v_batch_query TEXT;

    -- Seek state
    v_next_seek TEXT;
    v_count INT := 0;
    v_skipped INT := 0;
    v_has_pending_peek BOOLEAN := FALSE;
    v_emitted_folders TEXT[] := ARRAY[]::TEXT[];
BEGIN
    v_limit := LEAST(coalesce(limits, 100), 1500);
    v_prefix := coalesce(prefix, '') || coalesce(search, '');
    -- The caller may have LIKE-escaped the prefix (e.g. \_  \%).
    -- Keep the escaped version for ILIKE filters, but strip the
    -- backslash escapes for exact-match helper functions.
    v_raw_prefix := replace(replace(v_prefix, '\%', '%'), '\_', '_');
    v_prefix_lower := lower(v_prefix);
    v_is_asc := lower(coalesce(sortorder, 'asc')) = 'asc';
    v_file_batch_size := LEAST(GREATEST(v_limit * 2, 100), 1000);

    CASE lower(coalesce(sortcolumn, 'name'))
        WHEN 'name' THEN v_order_by := 'name';
        WHEN 'updated_at' THEN v_order_by := 'updated_at';
        WHEN 'created_at' THEN v_order_by := 'created_at';
        WHEN 'last_accessed_at' THEN v_order_by := 'last_accessed_at';
        ELSE v_order_by := 'name';
    END CASE;

    v_sort_order := CASE WHEN v_is_asc THEN 'asc' ELSE 'desc' END;

    IF v_order_by != 'name' THEN
        RETURN QUERY EXECUTE format(
            $sql$
            WITH folders AS (
                SELECT storage.get_prefix_child_name(objects.name, $5, '/') AS folder
                FROM storage.objects
                WHERE objects.name ILIKE $1 || '%%'
                  AND bucket_id = $2
                  AND storage.get_common_prefix(objects.name, $5, '/') IS NOT NULL
                GROUP BY folder
            ), entries AS (
                SELECT folder AS "name",
                       NULL::uuid AS id,
                       NULL::timestamptz AS updated_at,
                       NULL::timestamptz AS created_at,
                       NULL::timestamptz AS last_accessed_at,
                       NULL::jsonb AS metadata,
                       0 AS sort_group
                FROM folders
                WHERE folder IS NOT NULL
                UNION ALL
                SELECT storage.get_prefix_child_name(objects.name, $5, '/') AS "name",
                       id, updated_at, created_at, last_accessed_at, metadata,
                       1 AS sort_group
                FROM storage.objects
                WHERE objects.name ILIKE $1 || '%%'
                  AND bucket_id = $2
                  AND storage.get_common_prefix(objects.name, $5, '/') IS NULL
                  AND storage.get_prefix_child_name(objects.name, $5, '/') IS NOT NULL
            )
            SELECT "name", id, updated_at, created_at, last_accessed_at, metadata
            FROM entries
            ORDER BY sort_group ASC,
                     CASE WHEN sort_group = 0 THEN "name" END %s,
                     CASE WHEN sort_group = 1 THEN %I END %s,
                     CASE WHEN sort_group = 1 THEN "name" END %s
            LIMIT $3 OFFSET $4
            $sql$, v_sort_order, v_order_by, v_sort_order, v_sort_order
        ) USING v_prefix, bucketname, v_limit, offsets, v_raw_prefix;
        RETURN;
    END IF;

    IF v_prefix_lower = '' THEN
        v_upper_bound := NULL;
    ELSIF right(v_prefix_lower, 1) = v_delimiter THEN
        v_upper_bound := left(v_prefix_lower, -1) || chr(ascii(v_delimiter) + 1);
    ELSE
        v_upper_bound := left(v_prefix_lower, -1) || chr(ascii(right(v_prefix_lower, 1)) + 1);
    END IF;

    IF v_is_asc THEN
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" >= $2 ' ||
                'AND lower(o.name) COLLATE "C" < $3 ORDER BY lower(o.name) COLLATE "C" ASC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" >= $2 ' ||
                'ORDER BY lower(o.name) COLLATE "C" ASC LIMIT $4';
        END IF;
    ELSE
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" < $2 ' ||
                'AND lower(o.name) COLLATE "C" >= $3 ORDER BY lower(o.name) COLLATE "C" DESC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" < $2 ' ||
                'ORDER BY lower(o.name) COLLATE "C" DESC LIMIT $4';
        END IF;
    END IF;

    IF v_is_asc THEN
        v_next_seek := v_prefix_lower;
    ELSE
        IF v_upper_bound IS NOT NULL THEN
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_prefix_lower AND lower(o.name) COLLATE "C" < v_upper_bound
            ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
        ELSIF v_prefix_lower <> '' THEN
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_prefix_lower
            ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
        ELSE
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname
            ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
        END IF;

        IF v_peek_name IS NOT NULL THEN
            v_next_seek := lower(v_peek_name) || v_delimiter;
        ELSE
            RETURN;
        END IF;
    END IF;

    LOOP
        EXIT WHEN v_count >= v_limit;

        IF NOT v_has_pending_peek THEN
            IF v_is_asc THEN
                IF v_upper_bound IS NOT NULL THEN
                    SELECT o.name INTO v_peek_name FROM storage.objects o
                    WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_next_seek AND lower(o.name) COLLATE "C" < v_upper_bound
                    ORDER BY lower(o.name) COLLATE "C" ASC LIMIT 1;
                ELSE
                    SELECT o.name INTO v_peek_name FROM storage.objects o
                    WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_next_seek
                    ORDER BY lower(o.name) COLLATE "C" ASC LIMIT 1;
                END IF;
            ELSE
                IF v_upper_bound IS NOT NULL THEN
                    SELECT o.name INTO v_peek_name FROM storage.objects o
                    WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek AND lower(o.name) COLLATE "C" >= v_prefix_lower
                    ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
                ELSIF v_prefix_lower <> '' THEN
                    SELECT o.name INTO v_peek_name FROM storage.objects o
                    WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek AND lower(o.name) COLLATE "C" >= v_prefix_lower
                    ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
                ELSE
                    SELECT o.name INTO v_peek_name FROM storage.objects o
                    WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek
                    ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
                END IF;
            END IF;
        END IF;

        v_has_pending_peek := FALSE;

        EXIT WHEN v_peek_name IS NULL;

        v_common_prefix := storage.get_common_prefix(lower(v_peek_name), v_prefix_lower, v_delimiter);

        IF v_common_prefix IS NOT NULL THEN
            v_child_name := storage.get_prefix_child_name(v_peek_name, v_prefix, v_delimiter);

            IF v_child_name IS NOT NULL AND array_position(v_emitted_folders, v_child_name) IS NULL THEN
                v_emitted_folders := array_append(v_emitted_folders, v_child_name);

                IF v_skipped < offsets THEN
                    v_skipped := v_skipped + 1;
                ELSE
                    name := v_child_name;
                    id := NULL;
                    updated_at := NULL;
                    created_at := NULL;
                    last_accessed_at := NULL;
                    metadata := NULL;
                    RETURN NEXT;
                    v_count := v_count + 1;
                END IF;
            END IF;

            IF v_is_asc THEN
                v_next_seek := lower(left(v_common_prefix, -1)) || chr(ascii(v_delimiter) + 1);
            ELSE
                v_next_seek := lower(v_common_prefix);
            END IF;
        ELSE
            FOR v_current IN EXECUTE v_batch_query
                USING bucketname, v_next_seek,
                    CASE WHEN v_is_asc THEN COALESCE(v_upper_bound, v_prefix_lower) ELSE v_prefix_lower END, v_file_batch_size
            LOOP
                v_common_prefix := storage.get_common_prefix(lower(v_current.name), v_prefix_lower, v_delimiter);

                IF v_common_prefix IS NOT NULL THEN
                    v_peek_name := v_current.name;
                    v_has_pending_peek := TRUE;
                    EXIT;
                END IF;

                IF v_skipped < offsets THEN
                    v_skipped := v_skipped + 1;
                ELSE
                    name := storage.get_prefix_child_name(v_current.name, v_prefix, v_delimiter);
                    IF name IS NOT NULL THEN
                        id := v_current.id;
                        updated_at := v_current.updated_at;
                        created_at := v_current.created_at;
                        last_accessed_at := v_current.last_accessed_at;
                        metadata := v_current.metadata;
                        RETURN NEXT;
                        v_count := v_count + 1;
                    END IF;
                END IF;

                IF v_is_asc THEN
                    v_next_seek := lower(v_current.name) || v_delimiter;
                ELSE
                    v_next_seek := lower(v_current.name);
                END IF;

                EXIT WHEN v_count >= v_limit;
            END LOOP;
        END IF;
    END LOOP;
END;
$func$;
