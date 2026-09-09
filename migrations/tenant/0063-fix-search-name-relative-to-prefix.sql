-- fixes issue with search param not returning name relative to prefix; the level input is
-- no longer used (both boundaries are now derived internally from prefix/search) but is kept
-- as an unused parameter so the signature stays backwards compatible across a rolling deploy
-- based on prefix case fix version defined in 0056 which is based on original version defined in 0050
--
-- Also fixes a correctness bug in this same skip-scan: two folders whose names differ only by
-- case (e.g. "my_Folder" vs "my_folder") sort adjacently once compared case-insensitively, and
-- the original algorithm's O(1) "jump past this folder" step used a case-insensitive bound - so
-- it silently jumped over the second folder's rows as if they belonged to the first, merging the
-- two into one in the results.
--
-- Why lower(name) was the sort/seek key in the first place: prefix/search need to match
-- case-insensitively (see the "case insensitive search should work" test below), and once the
-- WHERE bound is lower(name)-based, using the same expression for ORDER BY was the only way to
-- get an index-backed walk. The sort order piggybacked on the filter - it was never a deliberate
-- design choice, and it's the entire reason two case-variant siblings get forced to tie on the
-- primary sort key, which is what let the original bug exist at all.
--
-- storage.search_v2/list_objects_with_delimiter (migration 0050) already sorts and seeks purely
-- by name COLLATE "C" - no lower() anywhere - and has never had this bug, by construction:
-- distinct exact strings are never forced to tie in the first place, so there's nothing to prove
-- and nothing to check. The fix below brings this function's NAME-SORT branch in line with that
-- for the case that matters - prefix empty or ending at a delimiter, true for every plain "list
-- this folder" call, which is what real navigation and every benchmark in
-- performance/search-stress-test/ actually does. No new tables, triggers, or indexes: the walk
-- uses idx_objects_bucket_id_name (bucket_id, name COLLATE "C"), which already exists (migration
-- 0020). Two prior approaches were tried and discarded before this one - see
-- performance/search-stress-test/ALGORITHM-VERSIONS.md ("v3", "v4") for what they cost and why
-- they were replaced: v3 kept lower(name) as the sort key and paid an existence-check-and-resolve
-- step on every folder boundary walked; v4 replaced that with a per-bucket flag maintained by a
-- write-side trigger and table, avoiding the per-folder cost but adding schema, write-path
-- overhead, and a bucket-wide blast radius (one collision anywhere in a bucket put every listing
-- against that bucket on the slow path). This version needs none of that.
--
-- The one case that can't take the fast, uniform path: `search` supplying a partial,
-- non-delimiter-terminated suffix (e.g. search='F' against sibling folders FOO/Foo/foo) can match
-- several *unrelated* folders that merely share a prefix, each of which independently might have
-- its own case collision - there's no single boundary to resolve once, the way there is for a
-- clean prefix. That's a data-shaped, per-folder cost no matter the approach, so this case falls
-- back to the previous (v3) algorithm verbatim - correct, at v3's existing cost, for a query
-- shape that plain folder navigation (and the stress harness) never produces.
-- ============================================================================
-- search: Legacy function with offset-based pagination using hybrid skip-scan
-- ============================================================================
-- Maintains backwards compatibility with the original search function signature.
-- Uses HYBRID approach for optimal performance:
--   1. STATIC SQL peek for folder discovery (plan cached, very fast)
--   2. DYNAMIC SQL batch for files (overhead amortized over many rows)
-- Falls back to path_tokens approach for non-name sorting.
-- ============================================================================
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
    v_common_prefix_exact TEXT;
    v_folder_seek_bound TEXT;
    v_has_case_collision BOOLEAN;
    v_range_lo TEXT;
    v_range_hi TEXT;
    v_case_variant_folders TEXT[];
    v_folder_name TEXT;
    v_delimiter CONSTANT TEXT := '/';

    -- Configuration
    v_limit INT;
    v_prefix TEXT;
    v_prefix_lower TEXT;
    v_resolved_prefix TEXT;
    v_prefix_len INT;
    v_prefix_start INT;
    v_combined_levels INT;
    v_is_asc BOOLEAN;
    v_order_by TEXT;
    v_sort_order TEXT;
    v_upper_bound TEXT;
    v_file_batch_size INT;
    v_exists BOOLEAN;
    v_fuzzy_suffix BOOLEAN;

    -- Dynamic SQL for batch query only
    v_batch_query TEXT;

    -- Seek state
    v_next_seek TEXT;
    v_count INT := 0;
    v_skipped INT := 0;
BEGIN
    -- ========================================================================
    -- INITIALIZATION
    -- ========================================================================
    v_limit := LEAST(coalesce(limits, 100), 1500);
    v_prefix := coalesce(prefix, '') || coalesce(search, '');
    v_prefix_lower := lower(v_prefix);
    v_prefix_len := length(coalesce(prefix, ''));
    v_prefix_start := coalesce(array_length(string_to_array(coalesce(prefix, ''), v_delimiter), 1), 1);
    v_combined_levels := coalesce(array_length(string_to_array(v_prefix, v_delimiter), 1), 1);
    v_is_asc := lower(coalesce(sortorder, 'asc')) = 'asc';
    v_file_batch_size := LEAST(GREATEST(v_limit * 2, 100), 1000);

    -- Validate sort column
    CASE lower(coalesce(sortcolumn, 'name'))
        WHEN 'name' THEN v_order_by := 'name';
        WHEN 'updated_at' THEN v_order_by := 'updated_at';
        WHEN 'created_at' THEN v_order_by := 'created_at';
        WHEN 'last_accessed_at' THEN v_order_by := 'last_accessed_at';
        ELSE v_order_by := 'name';
    END CASE;

    v_sort_order := CASE WHEN v_is_asc THEN 'asc' ELSE 'desc' END;

    -- ========================================================================
    -- NON-NAME SORTING: Use path_tokens approach
    -- ========================================================================
    IF v_order_by != 'name' THEN
        RETURN QUERY EXECUTE format(
            $sql$
            WITH folders AS (
                SELECT array_to_string(path_tokens[$1:$2], '/') AS folder
                FROM storage.objects
                WHERE objects.name ILIKE $3 || '%%'
                  AND bucket_id = $4
                  AND array_length(objects.path_tokens, 1) <> $2
                GROUP BY folder
                ORDER BY folder %s
            )
            (SELECT folder AS "name",
                   NULL::uuid AS id,
                   NULL::timestamptz AS updated_at,
                   NULL::timestamptz AS created_at,
                   NULL::timestamptz AS last_accessed_at,
                   NULL::jsonb AS metadata FROM folders)
            UNION ALL
            (SELECT array_to_string(path_tokens[$1:$2], '/') AS "name",
                   id, updated_at, created_at, last_accessed_at, metadata
             FROM storage.objects
             WHERE objects.name ILIKE $3 || '%%'
               AND bucket_id = $4
               AND array_length(objects.path_tokens, 1) = $2
             ORDER BY %I %s)
            LIMIT $5 OFFSET $6
            $sql$, v_sort_order, v_order_by, v_sort_order
        ) USING v_prefix_start, v_combined_levels, v_prefix, bucketname, v_limit, offsets;
        RETURN;
    END IF;

    -- v_prefix ends mid-segment (search supplied a partial suffix, e.g. search='F')
    -- when it's non-empty and doesn't end at a delimiter. That's the one shape
    -- where "resolve the boundary once" (below) doesn't apply - see the header
    -- comment.
    v_fuzzy_suffix := v_prefix <> '' AND right(v_prefix, 1) <> v_delimiter;

    IF v_fuzzy_suffix THEN
        -- ====================================================================
        -- FALLBACK PATH: hybrid skip-scan in lower(name) order, with a
        -- per-folder existence-check-and-resolve for case collisions. Same
        -- shape as the fast path below, kept only for this one query shape.
        -- ====================================================================
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
                    'AND lower(o.name) COLLATE "C" < $3 ORDER BY lower(o.name) COLLATE "C" ASC, o.name COLLATE "C" ASC LIMIT $4';
            ELSE
                v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                    'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" >= $2 ' ||
                    'ORDER BY lower(o.name) COLLATE "C" ASC, o.name COLLATE "C" ASC LIMIT $4';
            END IF;
        ELSE
            IF v_upper_bound IS NOT NULL THEN
                v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                    'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" < $2 ' ||
                    'AND lower(o.name) COLLATE "C" >= $3 ORDER BY lower(o.name) COLLATE "C" DESC, o.name COLLATE "C" DESC LIMIT $4';
            ELSE
                v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                    'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" < $2 ' ||
                    'ORDER BY lower(o.name) COLLATE "C" DESC, o.name COLLATE "C" DESC LIMIT $4';
            END IF;
        END IF;

        IF v_is_asc THEN
            v_next_seek := v_prefix_lower;
        ELSE
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_prefix_lower AND lower(o.name) COLLATE "C" < v_upper_bound
                ORDER BY lower(o.name) COLLATE "C" DESC, o.name COLLATE "C" DESC LIMIT 1;
            ELSIF v_prefix_lower <> '' THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_prefix_lower
                ORDER BY lower(o.name) COLLATE "C" DESC, o.name COLLATE "C" DESC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname
                ORDER BY lower(o.name) COLLATE "C" DESC, o.name COLLATE "C" DESC LIMIT 1;
            END IF;

            IF v_peek_name IS NOT NULL THEN
                v_next_seek := lower(v_peek_name) || v_delimiter;
            ELSE
                RETURN;
            END IF;
        END IF;

        LOOP
            EXIT WHEN v_count >= v_limit;

            IF v_is_asc THEN
                IF v_upper_bound IS NOT NULL THEN
                    SELECT o.name INTO v_peek_name FROM storage.objects o
                    WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_next_seek AND lower(o.name) COLLATE "C" < v_upper_bound
                    ORDER BY lower(o.name) COLLATE "C" ASC, o.name COLLATE "C" ASC LIMIT 1;
                ELSE
                    SELECT o.name INTO v_peek_name FROM storage.objects o
                    WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_next_seek
                    ORDER BY lower(o.name) COLLATE "C" ASC, o.name COLLATE "C" ASC LIMIT 1;
                END IF;
            ELSE
                IF v_upper_bound IS NOT NULL THEN
                    SELECT o.name INTO v_peek_name FROM storage.objects o
                    WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek AND lower(o.name) COLLATE "C" >= v_prefix_lower
                    ORDER BY lower(o.name) COLLATE "C" DESC, o.name COLLATE "C" DESC LIMIT 1;
                ELSIF v_prefix_lower <> '' THEN
                    SELECT o.name INTO v_peek_name FROM storage.objects o
                    WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek AND lower(o.name) COLLATE "C" >= v_prefix_lower
                    ORDER BY lower(o.name) COLLATE "C" DESC, o.name COLLATE "C" DESC LIMIT 1;
                ELSE
                    SELECT o.name INTO v_peek_name FROM storage.objects o
                    WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek
                    ORDER BY lower(o.name) COLLATE "C" DESC, o.name COLLATE "C" DESC LIMIT 1;
                END IF;
            END IF;

            EXIT WHEN v_peek_name IS NULL;

            v_common_prefix := storage.get_common_prefix(lower(v_peek_name), v_prefix_lower, v_delimiter);

            IF v_common_prefix IS NOT NULL THEN
                v_common_prefix_exact := storage.get_common_prefix(v_peek_name, v_prefix, v_delimiter);

                IF v_skipped < offsets THEN
                    v_skipped := v_skipped + 1;
                ELSE
                    name := substring(rtrim(v_common_prefix_exact, v_delimiter) from v_prefix_len + 1);
                    id := NULL;
                    updated_at := NULL;
                    created_at := NULL;
                    last_accessed_at := NULL;
                    metadata := NULL;
                    RETURN NEXT;
                    v_count := v_count + 1;
                END IF;

                -- Advance seek past the folder range. The case-insensitive range
                -- spans the same two boundaries regardless of direction; ASC walks
                -- it low-to-high, DESC high-to-low.
                IF v_is_asc THEN
                    v_folder_seek_bound := lower(left(v_common_prefix, -1)) || chr(ascii(v_delimiter) + 1);
                    v_range_lo := v_next_seek;
                    v_range_hi := v_folder_seek_bound;
                ELSE
                    v_folder_seek_bound := lower(v_common_prefix);
                    v_range_lo := v_folder_seek_bound;
                    v_range_hi := v_next_seek;
                END IF;

                -- Rows are ordered case-insensitively here, so a sibling folder
                -- differing only by case can share this range with the one just
                -- emitted above. Cheap existence check first; only pay for
                -- resolving every OTHER distinct exact-case folder in the range on
                -- the rare occasions a collision is actually present.
                SELECT EXISTS (
                    SELECT 1 FROM storage.objects o
                    WHERE o.bucket_id = bucketname
                      AND lower(o.name) COLLATE "C" >= v_range_lo AND lower(o.name) COLLATE "C" < v_range_hi
                      AND left(o.name, length(v_common_prefix_exact)) <> v_common_prefix_exact
                ) INTO v_has_case_collision;

                IF v_has_case_collision THEN
                    IF v_is_asc THEN
                        SELECT array_agg(folder_exact ORDER BY folder_exact COLLATE "C" ASC)
                        INTO v_case_variant_folders
                        FROM (
                            SELECT DISTINCT storage.get_common_prefix(o.name, v_prefix, v_delimiter) AS folder_exact
                            FROM storage.objects o
                            WHERE o.bucket_id = bucketname
                              AND lower(o.name) COLLATE "C" >= v_range_lo AND lower(o.name) COLLATE "C" < v_range_hi
                              AND left(o.name, length(v_common_prefix_exact)) <> v_common_prefix_exact
                        ) t;
                    ELSE
                        SELECT array_agg(folder_exact ORDER BY folder_exact COLLATE "C" DESC)
                        INTO v_case_variant_folders
                        FROM (
                            SELECT DISTINCT storage.get_common_prefix(o.name, v_prefix, v_delimiter) AS folder_exact
                            FROM storage.objects o
                            WHERE o.bucket_id = bucketname
                              AND lower(o.name) COLLATE "C" >= v_range_lo AND lower(o.name) COLLATE "C" < v_range_hi
                              AND left(o.name, length(v_common_prefix_exact)) <> v_common_prefix_exact
                        ) t;
                    END IF;

                    FOREACH v_folder_name IN ARRAY v_case_variant_folders
                    LOOP
                        EXIT WHEN v_count >= v_limit;

                        IF v_skipped < offsets THEN
                            v_skipped := v_skipped + 1;
                        ELSE
                            name := substring(rtrim(v_folder_name, v_delimiter) from v_prefix_len + 1);
                            id := NULL;
                            updated_at := NULL;
                            created_at := NULL;
                            last_accessed_at := NULL;
                            metadata := NULL;
                            RETURN NEXT;
                            v_count := v_count + 1;
                        END IF;
                    END LOOP;
                END IF;

                v_next_seek := v_folder_seek_bound;
            ELSE
                FOR v_current IN EXECUTE v_batch_query
                    USING bucketname, v_next_seek,
                        CASE WHEN v_is_asc THEN COALESCE(v_upper_bound, v_prefix_lower) ELSE v_prefix_lower END, v_file_batch_size
                LOOP
                    v_common_prefix := storage.get_common_prefix(lower(v_current.name), v_prefix_lower, v_delimiter);

                    IF v_common_prefix IS NOT NULL THEN
                        v_next_seek := lower(v_current.name);
                        EXIT;
                    END IF;

                    IF v_skipped < offsets THEN
                        v_skipped := v_skipped + 1;
                    ELSE
                        name := substring(v_current.name from v_prefix_len + 1);
                        id := v_current.id;
                        updated_at := v_current.updated_at;
                        created_at := v_current.created_at;
                        last_accessed_at := v_current.last_accessed_at;
                        metadata := v_current.metadata;
                        RETURN NEXT;
                        v_count := v_count + 1;
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

        RETURN;
    END IF;

    -- ========================================================================
    -- FAST PATH: prefix is empty or ends at a delimiter - true for every plain
    -- folder listing, which is what real navigation and every benchmark in
    -- performance/search-stress-test/ actually does. Walked entirely in
    -- exact-name order; no lower() anywhere below this point.
    -- ========================================================================

    -- Resolve the caller's prefix to the real casing it exists under, ONCE,
    -- before the walk starts - not on every comparison during it. Fast path:
    -- the caller's literal bytes already match something (the common case -
    -- clients get folder names from a prior listing, so they already have the
    -- right case), costing exactly one existence check. Only if that finds
    -- nothing does this fall back to a single lower(name)-indexed peek
    -- (idx_objects_bucket_id_name_lower) to find the real casing - one query,
    -- regardless of how large the matched subtree turns out to be, since it
    -- only needs ANY one matching row, not proof there's only one. Safe here
    -- specifically because v_prefix is delimiter-terminated (or empty): the
    -- character being bumped for the exact-match attempt is the delimiter
    -- itself, which has no case variants, so "exact match found nothing" and
    -- "exact match found the wrong casing entirely" are the only outcomes -
    -- unlike a partial suffix, there's no third case of "found some but not
    -- all matching casings" to miss.
    v_resolved_prefix := v_prefix;

    IF v_prefix <> '' THEN
        IF right(v_prefix, 1) = v_delimiter THEN
            v_upper_bound := left(v_prefix, -1) || chr(ascii(v_delimiter) + 1);
        ELSE
            v_upper_bound := left(v_prefix, -1) || chr(ascii(right(v_prefix, 1)) + 1);
        END IF;

        SELECT true INTO v_exists FROM storage.objects o
        WHERE o.bucket_id = bucketname AND o.name COLLATE "C" >= v_prefix AND o.name COLLATE "C" < v_upper_bound
        LIMIT 1;

        IF v_exists IS NULL THEN
            DECLARE
                v_lower_upper_bound TEXT;
            BEGIN
                IF right(v_prefix_lower, 1) = v_delimiter THEN
                    v_lower_upper_bound := left(v_prefix_lower, -1) || chr(ascii(v_delimiter) + 1);
                ELSE
                    v_lower_upper_bound := left(v_prefix_lower, -1) || chr(ascii(right(v_prefix_lower, 1)) + 1);
                END IF;

                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_prefix_lower
                  AND lower(o.name) COLLATE "C" < v_lower_upper_bound
                ORDER BY lower(o.name) COLLATE "C" ASC LIMIT 1;

                IF v_peek_name IS NULL THEN
                    RETURN; -- nothing matches this prefix under any casing
                END IF;

                v_resolved_prefix := left(v_peek_name, length(v_prefix));
            END;

            -- Recompute the exact-space bound against the resolved (real) casing -
            -- same length as v_prefix, but the bump depends on the actual last
            -- character, which resolution may have changed the case of.
            IF right(v_resolved_prefix, 1) = v_delimiter THEN
                v_upper_bound := left(v_resolved_prefix, -1) || chr(ascii(v_delimiter) + 1);
            ELSE
                v_upper_bound := left(v_resolved_prefix, -1) || chr(ascii(right(v_resolved_prefix, 1)) + 1);
            END IF;
        END IF;
    ELSE
        v_upper_bound := NULL;
    END IF;

    -- Build batch query (dynamic SQL - called infrequently, amortized over many
    -- rows). name COLLATE "C" alone: no secondary tie-break needed since name is
    -- already unique per bucket (bucketid_objname) - unlike lower(name), it can
    -- never tie, so pagination is deterministic for free.
    IF v_is_asc THEN
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND o.name COLLATE "C" >= $2 ' ||
                'AND o.name COLLATE "C" < $3 ORDER BY o.name COLLATE "C" ASC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND o.name COLLATE "C" >= $2 ' ||
                'ORDER BY o.name COLLATE "C" ASC LIMIT $4';
        END IF;
    ELSE
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND o.name COLLATE "C" < $2 ' ||
                'AND o.name COLLATE "C" >= $3 ORDER BY o.name COLLATE "C" DESC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND o.name COLLATE "C" < $2 ' ||
                'ORDER BY o.name COLLATE "C" DESC LIMIT $4';
        END IF;
    END IF;

    -- Initialize seek position
    IF v_is_asc THEN
        v_next_seek := v_resolved_prefix;
    ELSE
        -- DESC: find the last item in range first (static SQL)
        IF v_upper_bound IS NOT NULL THEN
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname AND o.name COLLATE "C" >= v_resolved_prefix AND o.name COLLATE "C" < v_upper_bound
            ORDER BY o.name COLLATE "C" DESC LIMIT 1;
        ELSIF v_resolved_prefix <> '' THEN
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname AND o.name COLLATE "C" >= v_resolved_prefix
            ORDER BY o.name COLLATE "C" DESC LIMIT 1;
        ELSE
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname
            ORDER BY o.name COLLATE "C" DESC LIMIT 1;
        END IF;

        IF v_peek_name IS NOT NULL THEN
            v_next_seek := v_peek_name || v_delimiter;
        ELSE
            RETURN;
        END IF;
    END IF;

    -- ========================================================================
    -- MAIN LOOP: Hybrid peek-then-batch algorithm, entirely in exact-name order.
    -- A folder jump is the original O(1) unconditional jump, always, with no
    -- existence check of any kind: two case-variant folders are simply two
    -- different strings, each with its own O(1) jump whenever the walk reaches
    -- it - there is nothing to prove, because they were never forced to tie in
    -- the first place.
    -- ========================================================================
    LOOP
        EXIT WHEN v_count >= v_limit;

        -- STEP 1: PEEK using STATIC SQL (plan cached, very fast)
        IF v_is_asc THEN
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND o.name COLLATE "C" >= v_next_seek AND o.name COLLATE "C" < v_upper_bound
                ORDER BY o.name COLLATE "C" ASC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND o.name COLLATE "C" >= v_next_seek
                ORDER BY o.name COLLATE "C" ASC LIMIT 1;
            END IF;
        ELSE
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND o.name COLLATE "C" < v_next_seek AND o.name COLLATE "C" >= v_resolved_prefix
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            ELSIF v_resolved_prefix <> '' THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND o.name COLLATE "C" < v_next_seek AND o.name COLLATE "C" >= v_resolved_prefix
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND o.name COLLATE "C" < v_next_seek
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            END IF;
        END IF;

        EXIT WHEN v_peek_name IS NULL;

        -- STEP 2: Check if this is a FOLDER or FILE
        v_common_prefix := storage.get_common_prefix(v_peek_name, v_resolved_prefix, v_delimiter);

        IF v_common_prefix IS NOT NULL THEN
            -- FOLDER: Handle offset, emit if needed, skip to next folder
            IF v_skipped < offsets THEN
                v_skipped := v_skipped + 1;
            ELSE
                name := substring(rtrim(v_common_prefix, v_delimiter) from v_prefix_len + 1);
                id := NULL;
                updated_at := NULL;
                created_at := NULL;
                last_accessed_at := NULL;
                metadata := NULL;
                RETURN NEXT;
                v_count := v_count + 1;
            END IF;

            -- Advance seek past the folder range - O(1), unconditional, same as
            -- before the collision fix existed, except now it's actually correct:
            -- this bound only ever spans rows that share v_common_prefix's own
            -- exact bytes, because nothing here was ever compared case-insensitively.
            IF v_is_asc THEN
                v_next_seek := left(v_common_prefix, -1) || chr(ascii(v_delimiter) + 1);
            ELSE
                v_next_seek := v_common_prefix;
            END IF;
        ELSE
            -- FILE: Batch fetch using DYNAMIC SQL (overhead amortized over many rows)
            -- For ASC: upper_bound is the exclusive upper limit (< condition)
            -- For DESC: v_resolved_prefix is the inclusive lower limit (>= condition)
            FOR v_current IN EXECUTE v_batch_query
                USING bucketname, v_next_seek,
                    CASE WHEN v_is_asc THEN COALESCE(v_upper_bound, v_resolved_prefix) ELSE v_resolved_prefix END, v_file_batch_size
            LOOP
                v_common_prefix := storage.get_common_prefix(v_current.name, v_resolved_prefix, v_delimiter);

                IF v_common_prefix IS NOT NULL THEN
                    -- Hit a folder: exit batch, let peek handle it
                    v_next_seek := v_current.name;
                    EXIT;
                END IF;

                -- Handle offset skipping
                IF v_skipped < offsets THEN
                    v_skipped := v_skipped + 1;
                ELSE
                    -- Emit file
                    name := substring(v_current.name from v_prefix_len + 1);
                    id := v_current.id;
                    updated_at := v_current.updated_at;
                    created_at := v_current.created_at;
                    last_accessed_at := v_current.last_accessed_at;
                    metadata := v_current.metadata;
                    RETURN NEXT;
                    v_count := v_count + 1;
                END IF;

                -- Advance seek past this file
                IF v_is_asc THEN
                    v_next_seek := v_current.name || v_delimiter;
                ELSE
                    v_next_seek := v_current.name;
                END IF;

                EXIT WHEN v_count >= v_limit;
            END LOOP;
        END IF;
    END LOOP;
END;
$func$;
