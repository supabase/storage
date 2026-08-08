-- Bucket-level versioning state: disabled -> enabled -> suspended (never back to disabled).
ALTER TABLE storage.buckets ADD COLUMN IF NOT EXISTS versioning_status text NOT NULL DEFAULT 'disabled'
    CHECK (versioning_status IN ('disabled', 'enabled', 'suspended'));

-- Single-table versioning columns on storage.objects.
-- archived_at:      NULL = current version; non-null = when this row stopped being current.
-- is_delete_marker: TRUE = an empty placeholder row representing a DELETE, not real content.
-- is_versioned:     TRUE = row was created while bucket versioning was 'enabled'.
--                   FALSE = S3 "null version" equivalent (created while versioning was
--                   'disabled'/'suspended'); at most one FALSE row may exist per
--                   (bucket_id, name) at a time - enforced below.
ALTER TABLE storage.objects ADD COLUMN IF NOT EXISTS archived_at timestamptz;
ALTER TABLE storage.objects ADD COLUMN IF NOT EXISTS is_delete_marker boolean NOT NULL DEFAULT false;
ALTER TABLE storage.objects ADD COLUMN IF NOT EXISTS is_versioned boolean NOT NULL DEFAULT false;

-- id is copied verbatim across every version of a key (immutable external contract),
-- so multiple rows for the same key now legitimately share the same id - id can no
-- longer be a PRIMARY KEY. Row identity moves to (bucket_id, name, version).
ALTER TABLE storage.objects DROP CONSTRAINT objects_pkey;

-- ADD CONSTRAINT has no IF NOT EXISTS guard in Postgres - wrapped defensively so a
-- migration re-run doesn't hard-fail on a duplicate constraint.
DO $$
BEGIN
    ALTER TABLE storage.objects
        ADD CONSTRAINT objects_bucket_id_name_version_key UNIQUE NULLS NOT DISTINCT (bucket_id, name, version);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- This plain unique index on (bucket_id, name) predates versioning (migration 0002)
-- and is incompatible with multiple rows per key - must be dropped so that inserting
-- a second (historical) row for an existing key doesn't violate it.
DROP INDEX IF EXISTS storage.bucketid_objname;

-- Only one "current" row per key at a time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_current_version
    ON storage.objects (bucket_id, name) WHERE archived_at IS NULL;

-- Only one "null version" (is_versioned = false) row per key, ever - matching
-- S3, where the null version is a single slot that gets reused/revived by
-- upsertObject (updated in place, including un-archiving it) rather than a
-- new row ever being inserted once one exists for a key.
CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_null_version
    ON storage.objects (bucket_id, name) WHERE NOT is_versioned;

-- For lifecycle/expiration queries
CREATE INDEX IF NOT EXISTS idx_objects_bucket_id_archived_at
    ON storage.objects (bucket_id, archived_at);

-- For sorting of a key's noncurrent_versions in order (created_at now gets updated on every
-- version change)
CREATE INDEX IF NOT EXISTS idx_objects_bucket_id_name_created_at
    ON storage.objects (bucket_id, name, created_at DESC);

-- ============================================================================
-- Function updates: every function below used to assume "one row per key" in
-- storage.objects. That's no longer true - a key can now have historical
-- (archived_at IS NOT NULL) rows and delete-marker rows alongside its current
-- row. Each function gets two new optional filter parameters, both defaulting
-- to their "current, live objects only" value so existing callers see exactly
-- the same rows as before:
-- Both use the same three-value shape, deliberately kept consistent:
--   noncurrent_versions ('exclude' | 'include' | 'only', default 'exclude'):
--     'exclude' considers only the current row (archived_at IS NULL, today's
--     behavior); 'only' flips this to ONLY archived/historical rows (an "IS"
--     filter, not an "include" toggle - lets a caller ask for just the
--     superseded versions of a key); 'include' considers both.
--   delete_markers ('exclude' | 'include' | 'only', default 'exclude'):
--     'exclude' is today's behavior (delete-marker rows never shown);
--     'include' shows them alongside everything else; 'only' is an "IS"
--     filter - ONLY delete-marker rows (e.g. combined with
--     noncurrent_versions='exclude', this answers "what's currently deleted").
-- Each function's output also gains version, archived_at, is_delete_marker,
-- and is_versioned columns - without them, a caller opting into
-- noncurrent_versions/delete_markers would get back multiple rows per key with no way to
-- tell them apart or know which is current. These are purely additive
-- (appended at the end of each RETURNS TABLE), safe for existing name-based
-- callers.
--
-- Sort: list_objects_with_delimiter gets a secondary "ORDER BY created_at
-- DESC" (and an extended (name, created_at, version) seek/cursor comparison)
-- so that when noncurrent_versions/delete_markers can produce more than one row per key,
-- those rows come back most-recent-first and pagination can resume mid-key
-- without skipping or duplicating rows - both only take effect when more than
-- one row per key is actually possible (noncurrent_versions IN ('only', 'include')),
-- so this is a no-op for every existing caller, which never sees that case.
--
-- search_by_timestamp (used by search_v2 whenever sort_column != 'name') gets
-- a narrower version of the same fix: its existing ROW(timestamp, name)
-- pagination seek already handled ties between DIFFERENT keys sharing a
-- timestamp (name always differs), but two VERSIONS of the SAME key sharing
-- a millisecond-truncated timestamp is a new possibility this feature
-- introduces (name is identical for both), so version is added as a third
-- ROW-comparison/ORDER BY tiebreak component. No peek/batch state machine
-- needed here (unlike list_objects_with_delimiter) - search_by_timestamp's
-- ordering was never grouped by key to begin with (it's a flat chronological
-- order across the whole bucket), so there's no "resume mid-key" concept,
-- just a tie that needed one more component to fully disambiguate.
--
-- TODO(object-versioning): storage.can_insert_object does a raw
-- INSERT + rollback permission-check trick with no version/archived_at/
-- is_versioned set. That insert could now collide with idx_objects_current_version
-- if a real current row already exists at that key. Not touched here since grep
-- found zero callers in this codebase's TypeScript - but if anything external
-- calls it directly via RPC, it's a live landmine. Follow up separately.
-- ============================================================================
--
-- CREATE OR REPLACE FUNCTION only replaces a function whose argument list is an
-- EXACT match - adding new trailing parameters (even with defaults) makes
-- Postgres create a second overload instead, leaving the old signature in
-- place and making every existing call site ambiguous. Each new signature
-- below is preceded by an explicit DROP FUNCTION of the exact old signature.

DROP FUNCTION IF EXISTS storage.list_objects_with_delimiter(text, text, text, integer, text, text, text);

CREATE OR REPLACE FUNCTION storage.list_objects_with_delimiter(
    _bucket_id text,
    prefix_param text,
    delimiter_param text,
    max_keys integer DEFAULT 100,
    start_after text DEFAULT ''::text,
    next_token text DEFAULT ''::text,
    sort_order text DEFAULT 'asc'::text,
    noncurrent_versions text DEFAULT 'exclude'::text,
    delete_markers text DEFAULT 'exclude'::text,
    next_token_created_at timestamptz DEFAULT NULL
)
 RETURNS TABLE(name text, id uuid, metadata jsonb, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, version text, archived_at timestamp with time zone, is_delete_marker boolean, is_versioned boolean)
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
    v_peek_name TEXT;
    v_current RECORD;
    v_common_prefix TEXT;

    -- Configuration
    v_is_asc BOOLEAN;
    v_prefix TEXT;
    v_start TEXT;
    v_upper_bound TEXT;
    v_file_batch_size INT;
    v_version_filter TEXT;

    -- noncurrent_versions IN ('only', 'include') is the only case where a single name
    -- can have more than one row - everything below that's conditioned on
    -- v_multi_row exists to keep those rows ordered (most-recent-first) and to
    -- let pagination resume mid-key. It's a no-op for every other caller
    -- (v_multi_row = false), which keeps the exact original single-row seek
    -- logic untouched.
    v_multi_row BOOLEAN;
    v_seek_lower_asc TEXT;
    v_seek_lower_desc TEXT;

    -- Seek state
    v_next_seek TEXT;
    v_next_seek_at TIMESTAMPTZ;
    v_count INT := 0;

    -- Dynamic SQL for batch query only
    v_batch_query TEXT;

BEGIN
    -- ========================================================================
    -- INITIALIZATION
    -- ========================================================================
    v_is_asc := lower(coalesce(sort_order, 'asc')) = 'asc';
    v_prefix := coalesce(prefix_param, '');
    v_start := CASE WHEN coalesce(next_token, '') <> '' THEN next_token ELSE coalesce(start_after, '') END;
    v_file_batch_size := LEAST(GREATEST(max_keys * 2, 100), 1000);
    v_next_seek_at := NULL;

    v_version_filter := '';
    IF noncurrent_versions = 'exclude' THEN
        v_version_filter := v_version_filter || ' AND o.archived_at IS NULL';
    ELSIF noncurrent_versions = 'only' THEN
        v_version_filter := v_version_filter || ' AND o.archived_at IS NOT NULL';
    END IF;
    IF delete_markers = 'exclude' THEN
        v_version_filter := v_version_filter || ' AND NOT o.is_delete_marker';
    ELSIF delete_markers = 'only' THEN
        v_version_filter := v_version_filter || ' AND o.is_delete_marker';
    END IF;

    -- Only 'only'/'include' can ever produce >1 row for the same name - see
    -- the v_multi_row declaration comment above for why everything gated on
    -- this is otherwise a no-op.
    v_multi_row := noncurrent_versions IN ('only', 'include');
    IF v_multi_row THEN
        -- Continue the same name (still emitting its remaining rows,
        -- most-recent-first) until created_at is exhausted, THEN move to the
        -- next name - $5 is the tiebreak timestamp (NULL means "haven't
        -- emitted anything for this name yet, take everything").
        v_seek_lower_asc := '(o.name COLLATE "C" > $2 OR (o.name COLLATE "C" = $2 AND ($5::timestamptz IS NULL OR o.created_at < $5)))';
        v_seek_lower_desc := '(o.name COLLATE "C" < $2 OR (o.name COLLATE "C" = $2 AND ($5::timestamptz IS NULL OR o.created_at < $5)))';
    ELSE
        v_seek_lower_asc := 'o.name COLLATE "C" >= $2';
        v_seek_lower_desc := 'o.name COLLATE "C" < $2';
    END IF;

    -- Calculate upper bound for prefix filtering (bytewise, using COLLATE "C")
    IF v_prefix = '' THEN
        v_upper_bound := NULL;
    ELSIF right(v_prefix, 1) = delimiter_param THEN
        v_upper_bound := left(v_prefix, -1) || chr(ascii(delimiter_param) + 1);
    ELSE
        v_upper_bound := left(v_prefix, -1) || chr(ascii(right(v_prefix, 1)) + 1);
    END IF;

    -- Build batch query (dynamic SQL - called infrequently, amortized over many rows)
    -- Secondary "ORDER BY o.created_at DESC" is unconditional and safe even
    -- when v_multi_row is false - with at most one row per name there's
    -- nothing to break a tie on, so it's a no-op for every existing caller.
    IF v_is_asc THEN
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata, o.version, o.archived_at, o.is_delete_marker, o.is_versioned ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND ' || v_seek_lower_asc || ' ' ||
                'AND o.name COLLATE "C" < $3' || v_version_filter || ' ORDER BY o.name COLLATE "C" ASC, o.created_at DESC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata, o.version, o.archived_at, o.is_delete_marker, o.is_versioned ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND ' || v_seek_lower_asc ||
                v_version_filter || ' ORDER BY o.name COLLATE "C" ASC, o.created_at DESC LIMIT $4';
        END IF;
    ELSE
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata, o.version, o.archived_at, o.is_delete_marker, o.is_versioned ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND ' || v_seek_lower_desc || ' ' ||
                'AND o.name COLLATE "C" >= $3' || v_version_filter || ' ORDER BY o.name COLLATE "C" DESC, o.created_at DESC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata, o.version, o.archived_at, o.is_delete_marker, o.is_versioned ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND ' || v_seek_lower_desc ||
                v_version_filter || ' ORDER BY o.name COLLATE "C" DESC, o.created_at DESC LIMIT $4';
        END IF;
    END IF;

    -- ========================================================================
    -- SEEK INITIALIZATION: Determine starting position
    -- ========================================================================
    IF v_start = '' THEN
        IF v_is_asc THEN
            v_next_seek := v_prefix;
        ELSE
            -- DESC without cursor: find the last item in range
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_next_seek FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND o.name COLLATE "C" >= v_prefix AND o.name COLLATE "C" < v_upper_bound
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            ELSIF v_prefix <> '' THEN
                SELECT o.name INTO v_next_seek FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND o.name COLLATE "C" >= v_prefix
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            ELSE
                SELECT o.name INTO v_next_seek FROM storage.objects o
                WHERE o.bucket_id = _bucket_id
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            END IF;

            IF v_next_seek IS NOT NULL THEN
                v_next_seek := v_next_seek || delimiter_param;
            ELSE
                RETURN;
            END IF;
        END IF;
    ELSE
        -- Cursor provided: determine if it refers to a folder or leaf
        IF EXISTS (
            SELECT 1 FROM storage.objects o
            WHERE o.bucket_id = _bucket_id
              AND o.name COLLATE "C" LIKE v_start || delimiter_param || '%'
              AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
              AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
              AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
              AND (delete_markers != 'only' OR o.is_delete_marker)
            LIMIT 1
        ) THEN
            -- Cursor refers to a folder
            IF v_is_asc THEN
                v_next_seek := v_start || chr(ascii(delimiter_param) + 1);
            ELSE
                v_next_seek := v_start || delimiter_param;
            END IF;
        ELSE
            -- Cursor refers to a leaf object. When v_multi_row, stay ON
            -- v_start (bare) with the caller-supplied tiebreak, so a page
            -- boundary that landed mid-key resumes that key's remaining rows
            -- instead of skipping them (see the batch loop's seek-advance
            -- comment for the matching within-call logic).
            IF v_multi_row THEN
                v_next_seek := v_start;
                v_next_seek_at := next_token_created_at;
            ELSIF v_is_asc THEN
                v_next_seek := v_start || delimiter_param;
            ELSE
                v_next_seek := v_start;
            END IF;
        END IF;
    END IF;

    -- ========================================================================
    -- MAIN LOOP: Hybrid peek-then-batch algorithm
    -- Uses STATIC SQL for peek (hot path) and DYNAMIC SQL for batch
    -- ========================================================================
    LOOP
        EXIT WHEN v_count >= max_keys;

        -- STEP 1: PEEK using STATIC SQL (plan cached, very fast)
        IF v_is_asc THEN
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND ((NOT v_multi_row AND o.name COLLATE "C" >= v_next_seek) OR (v_multi_row AND (o.name COLLATE "C" > v_next_seek OR (o.name COLLATE "C" = v_next_seek AND (v_next_seek_at IS NULL OR o.created_at < v_next_seek_at))))) AND o.name COLLATE "C" < v_upper_bound
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY o.name COLLATE "C" ASC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND ((NOT v_multi_row AND o.name COLLATE "C" >= v_next_seek) OR (v_multi_row AND (o.name COLLATE "C" > v_next_seek OR (o.name COLLATE "C" = v_next_seek AND (v_next_seek_at IS NULL OR o.created_at < v_next_seek_at)))))
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY o.name COLLATE "C" ASC LIMIT 1;
            END IF;
        ELSE
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND ((NOT v_multi_row AND o.name COLLATE "C" < v_next_seek) OR (v_multi_row AND (o.name COLLATE "C" < v_next_seek OR (o.name COLLATE "C" = v_next_seek AND (v_next_seek_at IS NULL OR o.created_at < v_next_seek_at))))) AND o.name COLLATE "C" >= v_prefix
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            ELSIF v_prefix <> '' THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND ((NOT v_multi_row AND o.name COLLATE "C" < v_next_seek) OR (v_multi_row AND (o.name COLLATE "C" < v_next_seek OR (o.name COLLATE "C" = v_next_seek AND (v_next_seek_at IS NULL OR o.created_at < v_next_seek_at))))) AND o.name COLLATE "C" >= v_prefix
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = _bucket_id AND ((NOT v_multi_row AND o.name COLLATE "C" < v_next_seek) OR (v_multi_row AND (o.name COLLATE "C" < v_next_seek OR (o.name COLLATE "C" = v_next_seek AND (v_next_seek_at IS NULL OR o.created_at < v_next_seek_at)))))
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY o.name COLLATE "C" DESC LIMIT 1;
            END IF;
        END IF;

        EXIT WHEN v_peek_name IS NULL;

        -- STEP 2: Check if this is a FOLDER or FILE
        v_common_prefix := storage.get_common_prefix(v_peek_name, v_prefix, delimiter_param);

        IF v_common_prefix IS NOT NULL THEN
            -- FOLDER: Emit and skip to next folder (no heap access needed)
            name := rtrim(v_common_prefix, delimiter_param);
            id := NULL;
            updated_at := NULL;
            created_at := NULL;
            last_accessed_at := NULL;
            metadata := NULL;
            version := NULL;
            archived_at := NULL;
            is_delete_marker := NULL;
            is_versioned := NULL;
            RETURN NEXT;
            v_count := v_count + 1;

            -- Advance seek past the folder range
            IF v_is_asc THEN
                v_next_seek := left(v_common_prefix, -1) || chr(ascii(delimiter_param) + 1);
            ELSE
                v_next_seek := v_common_prefix;
            END IF;
        ELSE
            -- FILE: Batch fetch using DYNAMIC SQL (overhead amortized over many rows)
            -- For ASC: upper_bound is the exclusive upper limit (< condition)
            -- For DESC: prefix is the inclusive lower limit (>= condition)
            FOR v_current IN EXECUTE v_batch_query USING _bucket_id, v_next_seek,
                CASE WHEN v_is_asc THEN COALESCE(v_upper_bound, v_prefix) ELSE v_prefix END, v_file_batch_size, v_next_seek_at
            LOOP
                v_common_prefix := storage.get_common_prefix(v_current.name, v_prefix, delimiter_param);

                IF v_common_prefix IS NOT NULL THEN
                    -- Hit a folder: exit batch, let peek handle it
                    v_next_seek := v_current.name;
                    v_next_seek_at := NULL;
                    EXIT;
                END IF;

                -- Emit file
                name := v_current.name;
                id := v_current.id;
                updated_at := v_current.updated_at;
                created_at := v_current.created_at;
                last_accessed_at := v_current.last_accessed_at;
                metadata := v_current.metadata;
                version := v_current.version;
                archived_at := v_current.archived_at;
                is_delete_marker := v_current.is_delete_marker;
                is_versioned := v_current.is_versioned;
                RETURN NEXT;
                v_count := v_count + 1;

                -- Advance seek past this file. When v_multi_row, stay ON this
                -- name (bare, no delimiter suffix) and record its created_at
                -- as the new tiebreak - the compound seek predicate above
                -- naturally picks up this name's remaining rows (older
                -- created_at) before ever moving to the next name, so pagination
                -- can resume correctly even if this exact row ends up being the
                -- last one emitted on this page.
                IF v_multi_row THEN
                    v_next_seek := v_current.name;
                    v_next_seek_at := v_current.created_at;
                ELSIF v_is_asc THEN
                    v_next_seek := v_current.name || delimiter_param;
                ELSE
                    v_next_seek := v_current.name;
                END IF;

                EXIT WHEN v_count >= max_keys;
            END LOOP;
        END IF;
    END LOOP;
END;
$function$;

DROP FUNCTION IF EXISTS storage.search(text, text, integer, integer, integer, text, text, text);

CREATE OR REPLACE FUNCTION storage.search(
    prefix text,
    bucketname text,
    limits integer DEFAULT 100,
    levels integer DEFAULT 1,
    offsets integer DEFAULT 0,
    search text DEFAULT ''::text,
    sortcolumn text DEFAULT 'name'::text,
    sortorder text DEFAULT 'asc'::text,
    noncurrent_versions text DEFAULT 'exclude'::text,
    delete_markers text DEFAULT 'exclude'::text
)
 RETURNS TABLE(name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb, version text, archived_at timestamp with time zone, is_delete_marker boolean, is_versioned boolean)
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
    v_peek_name TEXT;
    v_current RECORD;
    v_common_prefix TEXT;
    v_delimiter CONSTANT TEXT := '/';

    -- Configuration
    v_limit INT;
    v_prefix TEXT;
    v_prefix_lower TEXT;
    v_is_asc BOOLEAN;
    v_order_by TEXT;
    v_sort_order TEXT;
    v_upper_bound TEXT;
    v_file_batch_size INT;
    v_version_filter TEXT;

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
    v_is_asc := lower(coalesce(sortorder, 'asc')) = 'asc';
    v_file_batch_size := LEAST(GREATEST(v_limit * 2, 100), 1000);

    v_version_filter := '';
    IF noncurrent_versions = 'exclude' THEN
        v_version_filter := v_version_filter || ' AND o.archived_at IS NULL';
    ELSIF noncurrent_versions = 'only' THEN
        v_version_filter := v_version_filter || ' AND o.archived_at IS NOT NULL';
    END IF;
    IF delete_markers = 'exclude' THEN
        v_version_filter := v_version_filter || ' AND NOT o.is_delete_marker';
    ELSIF delete_markers = 'only' THEN
        v_version_filter := v_version_filter || ' AND o.is_delete_marker';
    END IF;

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
    -- NON-NAME SORTING: Use path_tokens approach (unchanged)
    -- ========================================================================
    IF v_order_by != 'name' THEN
        RETURN QUERY EXECUTE format(
            $sql$
            WITH folders AS (
                SELECT path_tokens[$1] AS folder
                FROM storage.objects
                WHERE objects.name ILIKE $2 || '%%'
                  AND bucket_id = $3
                  AND array_length(objects.path_tokens, 1) <> $1
                  AND ($6 != 'exclude' OR objects.archived_at IS NULL)
                  AND ($6 != 'only' OR objects.archived_at IS NOT NULL)
                  AND ($7 != 'exclude' OR NOT objects.is_delete_marker)
                  AND ($7 != 'only' OR objects.is_delete_marker)
                GROUP BY folder
                ORDER BY folder %s
            )
            (SELECT folder AS "name",
                   NULL::uuid AS id,
                   NULL::timestamptz AS updated_at,
                   NULL::timestamptz AS created_at,
                   NULL::timestamptz AS last_accessed_at,
                   NULL::jsonb AS metadata,
                   NULL::text AS version,
                   NULL::timestamptz AS archived_at,
                   NULL::boolean AS is_delete_marker,
                   NULL::boolean AS is_versioned FROM folders)
            UNION ALL
            (SELECT path_tokens[$1] AS "name",
                   id, updated_at, created_at, last_accessed_at, metadata,
                   version, archived_at, is_delete_marker, is_versioned
             FROM storage.objects
             WHERE objects.name ILIKE $2 || '%%'
               AND bucket_id = $3
               AND array_length(objects.path_tokens, 1) = $1
               AND ($6 != 'exclude' OR objects.archived_at IS NULL)
               AND ($6 != 'only' OR objects.archived_at IS NOT NULL)
               AND ($7 != 'exclude' OR NOT objects.is_delete_marker)
               AND ($7 != 'only' OR objects.is_delete_marker)
             -- name, then version, as tiebreaks - same reasoning as
             -- search_by_timestamp's fix: this is a flat order (not grouped
             -- by key), so version only matters for the narrow case of two
             -- versions of the SAME key tying on the sort column too (name
             -- alone no longer disambiguates once a key can have >1 row).
             -- offset-based (like the rest of this function), so no cursor/
             -- resume-mid-key concern, just full determinism across offsets.
             ORDER BY %I %s, name COLLATE "C" %s, COALESCE(version, '') %s)
            LIMIT $4 OFFSET $5
            $sql$, v_sort_order, v_order_by, v_sort_order, v_sort_order, v_sort_order
        ) USING levels, v_prefix, bucketname, v_limit, offsets, noncurrent_versions, delete_markers;
        RETURN;
    END IF;

    -- ========================================================================
    -- NAME SORTING: Hybrid skip-scan with batch optimization
    -- ========================================================================

    -- Calculate upper bound for prefix filtering
    IF v_prefix_lower = '' THEN
        v_upper_bound := NULL;
    ELSIF right(v_prefix_lower, 1) = v_delimiter THEN
        v_upper_bound := left(v_prefix_lower, -1) || chr(ascii(v_delimiter) + 1);
    ELSE
        v_upper_bound := left(v_prefix_lower, -1) || chr(ascii(right(v_prefix_lower, 1)) + 1);
    END IF;

    -- Build batch query (dynamic SQL - called infrequently, amortized over many rows)
    -- Secondary "ORDER BY o.created_at DESC" is unconditional and safe even
    -- when a name can only ever have one row - nothing to break a tie on, so
    -- it's a no-op for every existing caller. Unlike list_objects_with_delimiter,
    -- no seek/tiebreak changes are needed alongside it: search() paginates via
    -- offset (re-walking from the start every call), not an external resume
    -- cursor, so there's no "resume mid-key" case to handle - just needs a
    -- fully deterministic order so a given offset always means the same rows.
    IF v_is_asc THEN
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata, o.version, o.archived_at, o.is_delete_marker, o.is_versioned ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" >= $2 ' ||
                'AND lower(o.name) COLLATE "C" < $3' || v_version_filter || ' ORDER BY lower(o.name) COLLATE "C" ASC, o.created_at DESC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata, o.version, o.archived_at, o.is_delete_marker, o.is_versioned ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" >= $2' ||
                v_version_filter || ' ORDER BY lower(o.name) COLLATE "C" ASC, o.created_at DESC LIMIT $4';
        END IF;
    ELSE
        IF v_upper_bound IS NOT NULL THEN
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata, o.version, o.archived_at, o.is_delete_marker, o.is_versioned ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" < $2 ' ||
                'AND lower(o.name) COLLATE "C" >= $3' || v_version_filter || ' ORDER BY lower(o.name) COLLATE "C" DESC, o.created_at DESC LIMIT $4';
        ELSE
            v_batch_query := 'SELECT o.name, o.id, o.updated_at, o.created_at, o.last_accessed_at, o.metadata, o.version, o.archived_at, o.is_delete_marker, o.is_versioned ' ||
                'FROM storage.objects o WHERE o.bucket_id = $1 AND lower(o.name) COLLATE "C" < $2' ||
                v_version_filter || ' ORDER BY lower(o.name) COLLATE "C" DESC, o.created_at DESC LIMIT $4';
        END IF;
    END IF;

    -- Initialize seek position
    IF v_is_asc THEN
        v_next_seek := v_prefix_lower;
    ELSE
        -- DESC: find the last item in range first (static SQL)
        IF v_upper_bound IS NOT NULL THEN
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_prefix_lower AND lower(o.name) COLLATE "C" < v_upper_bound
              AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
              AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
              AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
              AND (delete_markers != 'only' OR o.is_delete_marker)
            ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
        ELSIF v_prefix_lower <> '' THEN
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_prefix_lower
              AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
              AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
              AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
              AND (delete_markers != 'only' OR o.is_delete_marker)
            ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
        ELSE
            SELECT o.name INTO v_peek_name FROM storage.objects o
            WHERE o.bucket_id = bucketname
              AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
              AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
              AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
              AND (delete_markers != 'only' OR o.is_delete_marker)
            ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
        END IF;

        IF v_peek_name IS NOT NULL THEN
            v_next_seek := lower(v_peek_name) || v_delimiter;
        ELSE
            RETURN;
        END IF;
    END IF;

    -- ========================================================================
    -- MAIN LOOP: Hybrid peek-then-batch algorithm
    -- Uses STATIC SQL for peek (hot path) and DYNAMIC SQL for batch
    -- ========================================================================
    LOOP
        EXIT WHEN v_count >= v_limit;

        -- STEP 1: PEEK using STATIC SQL (plan cached, very fast)
        IF v_is_asc THEN
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_next_seek AND lower(o.name) COLLATE "C" < v_upper_bound
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY lower(o.name) COLLATE "C" ASC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" >= v_next_seek
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY lower(o.name) COLLATE "C" ASC LIMIT 1;
            END IF;
        ELSE
            IF v_upper_bound IS NOT NULL THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek AND lower(o.name) COLLATE "C" >= v_prefix_lower
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
            ELSIF v_prefix_lower <> '' THEN
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek AND lower(o.name) COLLATE "C" >= v_prefix_lower
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
            ELSE
                SELECT o.name INTO v_peek_name FROM storage.objects o
                WHERE o.bucket_id = bucketname AND lower(o.name) COLLATE "C" < v_next_seek
                  AND (noncurrent_versions != 'exclude' OR o.archived_at IS NULL)
                  AND (noncurrent_versions != 'only' OR o.archived_at IS NOT NULL)
                  AND (delete_markers != 'exclude' OR NOT o.is_delete_marker)
                  AND (delete_markers != 'only' OR o.is_delete_marker)
                ORDER BY lower(o.name) COLLATE "C" DESC LIMIT 1;
            END IF;
        END IF;

        EXIT WHEN v_peek_name IS NULL;

        -- STEP 2: Check if this is a FOLDER or FILE
        v_common_prefix := storage.get_common_prefix(lower(v_peek_name), v_prefix_lower, v_delimiter);

        IF v_common_prefix IS NOT NULL THEN
            -- FOLDER: Handle offset, emit if needed, skip to next folder
            IF v_skipped < offsets THEN
                v_skipped := v_skipped + 1;
            ELSE
                name := split_part(rtrim(storage.get_common_prefix(v_peek_name, v_prefix, v_delimiter), v_delimiter), v_delimiter, levels);
                id := NULL;
                updated_at := NULL;
                created_at := NULL;
                last_accessed_at := NULL;
                metadata := NULL;
                version := NULL;
                archived_at := NULL;
                is_delete_marker := NULL;
                is_versioned := NULL;
                RETURN NEXT;
                v_count := v_count + 1;
            END IF;

            -- Advance seek past the folder range
            IF v_is_asc THEN
                v_next_seek := lower(left(v_common_prefix, -1)) || chr(ascii(v_delimiter) + 1);
            ELSE
                v_next_seek := lower(v_common_prefix);
            END IF;
        ELSE
            -- FILE: Batch fetch using DYNAMIC SQL (overhead amortized over many rows)
            -- For ASC: upper_bound is the exclusive upper limit (< condition)
            -- For DESC: prefix_lower is the inclusive lower limit (>= condition)
            FOR v_current IN EXECUTE v_batch_query
                USING bucketname, v_next_seek,
                    CASE WHEN v_is_asc THEN COALESCE(v_upper_bound, v_prefix_lower) ELSE v_prefix_lower END, v_file_batch_size
            LOOP
                v_common_prefix := storage.get_common_prefix(lower(v_current.name), v_prefix_lower, v_delimiter);

                IF v_common_prefix IS NOT NULL THEN
                    -- Hit a folder: exit batch, let peek handle it
                    v_next_seek := lower(v_current.name);
                    EXIT;
                END IF;

                -- Handle offset skipping
                IF v_skipped < offsets THEN
                    v_skipped := v_skipped + 1;
                ELSE
                    -- Emit file
                    name := split_part(v_current.name, v_delimiter, levels);
                    id := v_current.id;
                    updated_at := v_current.updated_at;
                    created_at := v_current.created_at;
                    last_accessed_at := v_current.last_accessed_at;
                    metadata := v_current.metadata;
                    version := v_current.version;
                    archived_at := v_current.archived_at;
                    is_delete_marker := v_current.is_delete_marker;
                    is_versioned := v_current.is_versioned;
                    RETURN NEXT;
                    v_count := v_count + 1;
                END IF;

                -- Advance seek past this file
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
$function$;

DROP FUNCTION IF EXISTS storage.search_by_timestamp(text, text, integer, integer, text, text, text, text);

CREATE OR REPLACE FUNCTION storage.search_by_timestamp(
    p_prefix text,
    p_bucket_id text,
    p_limit integer,
    p_level integer,
    p_start_after text,
    p_sort_order text,
    p_sort_column text,
    p_sort_column_after text,
    noncurrent_versions text DEFAULT 'exclude'::text,
    delete_markers text DEFAULT 'exclude'::text,
    p_start_after_version text DEFAULT ''::text
)
 RETURNS TABLE(key text, name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb, version text, archived_at timestamp with time zone, is_delete_marker boolean, is_versioned boolean)
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
    v_cursor_op text;
    v_query text;
    v_prefix text;
BEGIN
    v_prefix := coalesce(p_prefix, '');

    IF p_sort_order = 'asc' THEN
        v_cursor_op := '>';
    ELSE
        v_cursor_op := '<';
    END IF;

    v_query := format($sql$
        WITH raw_objects AS (
            SELECT
                o.name AS obj_name,
                o.id AS obj_id,
                o.updated_at AS obj_updated_at,
                o.created_at AS obj_created_at,
                o.last_accessed_at AS obj_last_accessed_at,
                o.metadata AS obj_metadata,
                o.version AS obj_version,
                o.archived_at AS obj_archived_at,
                o.is_delete_marker AS obj_is_delete_marker,
                o.is_versioned AS obj_is_versioned,
                storage.get_common_prefix(o.name, $1, '/') AS common_prefix
            FROM storage.objects o
            WHERE o.bucket_id = $2
              AND o.name COLLATE "C" LIKE $1 || '%%'
              AND ($7 != 'exclude' OR o.archived_at IS NULL)
              AND ($7 != 'only' OR o.archived_at IS NOT NULL)
              AND ($8 != 'exclude' OR NOT o.is_delete_marker)
              AND ($8 != 'only' OR o.is_delete_marker)
        ),
        -- Aggregate common prefixes (folders)
        -- Both created_at and updated_at use MIN(obj_created_at) to match the old prefixes table behavior
        aggregated_prefixes AS (
            SELECT
                rtrim(common_prefix, '/') AS name,
                NULL::uuid AS id,
                MIN(obj_created_at) AS updated_at,
                MIN(obj_created_at) AS created_at,
                NULL::timestamptz AS last_accessed_at,
                NULL::jsonb AS metadata,
                NULL::text AS version,
                NULL::timestamptz AS archived_at,
                NULL::boolean AS is_delete_marker,
                NULL::boolean AS is_versioned,
                TRUE AS is_prefix
            FROM raw_objects
            WHERE common_prefix IS NOT NULL
            GROUP BY common_prefix
        ),
        leaf_objects AS (
            SELECT
                obj_name AS name,
                obj_id AS id,
                obj_updated_at AS updated_at,
                obj_created_at AS created_at,
                obj_last_accessed_at AS last_accessed_at,
                obj_metadata AS metadata,
                obj_version AS version,
                obj_archived_at AS archived_at,
                obj_is_delete_marker AS is_delete_marker,
                obj_is_versioned AS is_versioned,
                FALSE AS is_prefix
            FROM raw_objects
            WHERE common_prefix IS NULL
        ),
        combined AS (
            SELECT * FROM aggregated_prefixes
            UNION ALL
            SELECT * FROM leaf_objects
        ),
        filtered AS (
            SELECT *
            FROM combined
            WHERE (
                $5 = ''
                OR ROW(
                    date_trunc('milliseconds', %I),
                    name COLLATE "C",
                    COALESCE(version, '')
                ) %s ROW(
                    -- Truncated the same way as the stored value above -
                    -- otherwise the boundary row's own (untruncated) cursor
                    -- value never equals its (truncated) stored value, and it
                    -- can incorrectly re-match on the next page.
                    date_trunc('milliseconds', COALESCE(NULLIF($6, '')::timestamptz, 'epoch'::timestamptz)),
                    $5,
                    $9
                )
            )
        )
        SELECT
            split_part(name, '/', $3) AS key,
            name,
            id,
            updated_at,
            created_at,
            last_accessed_at,
            metadata,
            version,
            archived_at,
            is_delete_marker,
            is_versioned
        FROM filtered
        ORDER BY
            COALESCE(date_trunc('milliseconds', %I), 'epoch'::timestamptz) %s,
            name COLLATE "C" %s,
            COALESCE(version, '') %s
        LIMIT $4
    $sql$,
        p_sort_column,
        v_cursor_op,
        p_sort_column,
        p_sort_order,
        p_sort_order,
        p_sort_order
    );

    -- version is the third tiebreak component (see filtered CTE / ORDER BY
    -- above) - only matters when two versions of the SAME key tie on both
    -- timestamp (millisecond-truncated) and name, which only becomes
    -- possible once a key can have more than one row. Without it, that tie
    -- would be genuinely ambiguous and a page boundary landing on it could
    -- skip or duplicate a row.
    RETURN QUERY EXECUTE v_query
    USING v_prefix, p_bucket_id, p_level, p_limit, p_start_after, p_sort_column_after, noncurrent_versions, delete_markers, coalesce(p_start_after_version, '');
END;
$function$;

DROP FUNCTION IF EXISTS storage.search_v2(text, text, integer, integer, text, text, text, text);

CREATE OR REPLACE FUNCTION storage.search_v2(
    prefix text,
    bucket_name text,
    limits integer DEFAULT 100,
    levels integer DEFAULT 1,
    start_after text DEFAULT ''::text,
    sort_order text DEFAULT 'asc'::text,
    sort_column text DEFAULT 'name'::text,
    sort_column_after text DEFAULT ''::text,
    noncurrent_versions text DEFAULT 'exclude'::text,
    delete_markers text DEFAULT 'exclude'::text,
    start_after_created_at timestamptz DEFAULT NULL,
    start_after_version text DEFAULT ''::text
)
 RETURNS TABLE(key text, name text, id uuid, updated_at timestamp with time zone, created_at timestamp with time zone, last_accessed_at timestamp with time zone, metadata jsonb, version text, archived_at timestamp with time zone, is_delete_marker boolean, is_versioned boolean)
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
    v_sort_col text;
    v_sort_ord text;
    v_limit int;
BEGIN
    -- Cap limit to maximum of 1500 records
    v_limit := LEAST(coalesce(limits, 100), 1500);

    -- Validate and normalize sort_order
    v_sort_ord := lower(coalesce(sort_order, 'asc'));
    IF v_sort_ord NOT IN ('asc', 'desc') THEN
        v_sort_ord := 'asc';
    END IF;

    -- Validate and normalize sort_column
    v_sort_col := lower(coalesce(sort_column, 'name'));
    IF v_sort_col NOT IN ('name', 'updated_at', 'created_at') THEN
        v_sort_col := 'name';
    END IF;

    -- Route to appropriate implementation
    IF v_sort_col = 'name' THEN
        -- Use list_objects_with_delimiter for name sorting (most efficient: O(k * log n))
        RETURN QUERY
        SELECT
            split_part(l.name, '/', levels) AS key,
            l.name AS name,
            l.id,
            l.updated_at,
            l.created_at,
            l.last_accessed_at,
            l.metadata,
            l.version,
            l.archived_at,
            l.is_delete_marker,
            l.is_versioned
        FROM storage.list_objects_with_delimiter(
            bucket_name,
            coalesce(prefix, ''),
            '/',
            v_limit,
            start_after,
            '',
            v_sort_ord,
            noncurrent_versions,
            delete_markers,
            start_after_created_at
        ) l;
    ELSE
        -- Use aggregation approach for timestamp sorting
        -- Not efficient for large datasets but supports correct pagination
        RETURN QUERY SELECT * FROM storage.search_by_timestamp(
            prefix, bucket_name, v_limit, levels, start_after,
            v_sort_ord, v_sort_col, sort_column_after,
            noncurrent_versions, delete_markers, start_after_version
        );
    END IF;
END;
$function$;

DROP FUNCTION IF EXISTS storage.get_size_by_bucket();

CREATE OR REPLACE FUNCTION storage.get_size_by_bucket(
    noncurrent_versions text DEFAULT 'exclude'::text,
    delete_markers text DEFAULT 'exclude'::text
)
 RETURNS TABLE(size bigint, bucket_id text)
 LANGUAGE plpgsql
 STABLE
AS $function$
BEGIN
    return query
        select sum((metadata->>'size')::bigint)::bigint as size, obj.bucket_id
        from "storage".objects as obj
        where (noncurrent_versions != 'exclude' OR obj.archived_at IS NULL)
          and (noncurrent_versions != 'only' OR obj.archived_at IS NOT NULL)
          and (delete_markers != 'exclude' OR NOT obj.is_delete_marker)
          and (delete_markers != 'only' OR obj.is_delete_marker)
        group by obj.bucket_id;
END
$function$;
