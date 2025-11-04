-- Drop existing triggers that will be replaced
DROP TRIGGER IF EXISTS objects_insert_create_prefix ON storage.objects;
DROP TRIGGER IF EXISTS objects_update_create_prefix ON storage.objects;
DROP TRIGGER IF EXISTS objects_delete_delete_prefix ON storage.objects;
DROP TRIGGER IF EXISTS prefixes_delete_hierarchy ON storage.prefixes;
DROP TRIGGER IF EXISTS prefixes_create_hierarchy ON storage.prefixes;

-- Add ref counting columns to prefixes table
-- Default to -1 (sentinel value meaning "not yet calculated")
-- Migration 0045 will calculate correct values and replace increment/decrement functions
ALTER TABLE storage.prefixes
    ADD COLUMN IF NOT EXISTS child_objects INTEGER DEFAULT -1,
    ADD COLUMN IF NOT EXISTS child_prefixes INTEGER DEFAULT -1;

-- Helper function to get the direct parent prefix
CREATE OR REPLACE FUNCTION storage.get_direct_parent(name text)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE STRICT
AS $$
    SELECT CASE
        WHEN position('/' in name) = 0 THEN ''
        ELSE regexp_replace(name, '/[^/]+$', '')
    END;
$$;

-- Function to acquire advisory lock for a specific bucket+prefix combination
-- This ensures serialization of operations on the same prefix to avoid race conditions
CREATE OR REPLACE FUNCTION storage.lock_prefix(bucket_id text, prefix_name text)
    RETURNS void
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(bucket_id || '/' || prefix_name, 0)
    );
END;
$$;

-- Function to acquire multiple prefix locks in consistent order to prevent deadlocks
-- This function takes a list of prefixes and locks them in deterministic order
CREATE OR REPLACE FUNCTION storage.lock_multiple_prefixes(bucket_id text, prefix_names text[])
    RETURNS void
    LANGUAGE plpgsql
AS $$
DECLARE
    _prefix_name text;
    _sorted_prefixes text[];
BEGIN
    -- Sort prefixes to ensure consistent lock ordering across all transactions
    SELECT array_agg(prefix_name ORDER BY prefix_name)
    INTO _sorted_prefixes
    FROM unnest(prefix_names) AS prefix_name
    WHERE prefix_name != '';

    -- Acquire locks in sorted order
    FOREACH _prefix_name IN ARRAY _sorted_prefixes
    LOOP
        PERFORM storage.lock_prefix(bucket_id, _prefix_name);
    END LOOP;
END;
$$;

-- Function to increment child_prefixes count for a parent prefix
-- TEMPORARY: During migration, just ensures prefix exists with -1 sentinel
-- This will be replaced with real increment logic at end of migration 0045
CREATE OR REPLACE FUNCTION storage.increment_prefix_child_count(
    _bucket_id text,
    _child_name text
)
    RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
AS $$
DECLARE
    _parent_name text;
BEGIN
    _parent_name := storage.get_direct_parent(_child_name);

    IF _parent_name = '' THEN
        RETURN;
    END IF;

    PERFORM storage.lock_prefix(_bucket_id, _parent_name);

    -- Just ensure prefix exists with -1 sentinel (don't increment yet)
    INSERT INTO storage.prefixes (bucket_id, name, child_objects, child_prefixes)
    VALUES (_bucket_id, _parent_name, -1, -1)
    ON CONFLICT (bucket_id, level, name) DO NOTHING;
END;
$$;

-- Function to decrement child_prefixes count for a parent prefix
-- TEMPORARY: During migration, this is a NO-OP (prefix should already exist)
-- This will be replaced with real decrement logic at end of migration 0045
CREATE OR REPLACE FUNCTION storage.decrement_prefix_child_count(
    _bucket_id text,
    _child_name text
)
    RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
AS $$
BEGIN
    RETURN;
END;
$$;

-- Function to increment child_objects count for a prefix
-- TEMPORARY: During migration, just ensures prefix exists with -1 sentinel
-- This will be replaced with real increment logic at end of migration 0045
CREATE OR REPLACE FUNCTION storage.increment_prefix_object_count(
    _bucket_id text,
    _prefix_name text,
    _count bigint
)
    RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
AS $$
BEGIN
    IF _prefix_name = '' THEN
        RETURN;
    END IF;

    -- Just ensure prefix exists with -1 sentinel (don't increment yet)
    INSERT INTO storage.prefixes (bucket_id, name, child_objects, child_prefixes)
    VALUES (_bucket_id, _prefix_name, -1, -1)
    ON CONFLICT (bucket_id, level, name) DO NOTHING;
END;
$$;

-- Function to decrement child_objects count for a prefix
-- TEMPORARY: During migration, this is a NO-OP
-- This will be replaced with real decrement logic at end of migration 0045
CREATE OR REPLACE FUNCTION storage.decrement_prefix_object_count(
    _bucket_id text,
    _prefix_name text,
    _count bigint
)
    RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
AS $$
BEGIN
    RETURN;
END;
$$;

-- Trigger function for object insertions (statement-level)
-- Creates parent prefixes and increments child_objects counts
CREATE OR REPLACE FUNCTION storage.objects_insert_after_ref_counting()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
AS $$
DECLARE
    _all_prefixes text[];
    _prefix_updates RECORD;
BEGIN
    -- Collect all unique prefixes and acquire locks upfront in sorted order to prevent deadlocks
    SELECT array_agg(DISTINCT storage.get_direct_parent(i.name) ORDER BY storage.get_direct_parent(i.name))
    INTO _all_prefixes
    FROM inserted i
    WHERE position('/' in i.name) > 0
      AND storage.get_direct_parent(i.name) != '';

    IF _all_prefixes IS NOT NULL AND array_length(_all_prefixes, 1) > 0 THEN
        FOR _prefix_updates IN
            SELECT DISTINCT bucket_id FROM inserted
        LOOP
            PERFORM storage.lock_multiple_prefixes(_prefix_updates.bucket_id, _all_prefixes);
        END LOOP;
    END IF;

    FOR _prefix_updates IN
        WITH inserted_with_parents AS (
            SELECT
                i.bucket_id,
                storage.get_direct_parent(i.name) as parent_prefix,
                COUNT(*) as inserted_count
            FROM inserted i
            WHERE position('/' in i.name) > 0
            GROUP BY i.bucket_id, storage.get_direct_parent(i.name)
        )
        SELECT bucket_id, parent_prefix, inserted_count
        FROM inserted_with_parents
        WHERE parent_prefix != ''
        ORDER BY bucket_id, parent_prefix
    LOOP
        PERFORM storage.increment_prefix_object_count(
            _prefix_updates.bucket_id,
            _prefix_updates.parent_prefix,
            _prefix_updates.inserted_count
        );
    END LOOP;

    RETURN NULL;
END;
$$;

-- Trigger function the level before insert or update object
CREATE OR REPLACE FUNCTION storage.objects_set_level()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    NEW.level := storage.get_level(NEW.name);
    RETURN NEW;
END;
$$;

-- Trigger function for object deletions (statement-level)
-- Decrements parent prefix/object counts
CREATE OR REPLACE FUNCTION storage.objects_delete_ref_counting()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
AS $$
DECLARE
    _prefix_updates RECORD;
    _all_prefixes text[];
BEGIN
    -- Collect all unique prefixes and acquire locks upfront in sorted order to prevent deadlocks
    SELECT array_agg(DISTINCT storage.get_direct_parent(d.name) ORDER BY storage.get_direct_parent(d.name))
    INTO _all_prefixes
    FROM deleted d
    WHERE position('/' in d.name) > 0
      AND storage.get_direct_parent(d.name) != '';

    IF _all_prefixes IS NOT NULL AND array_length(_all_prefixes, 1) > 0 THEN
        FOR _prefix_updates IN
            SELECT DISTINCT bucket_id FROM deleted
        LOOP
            PERFORM storage.lock_multiple_prefixes(_prefix_updates.bucket_id, _all_prefixes);
        END LOOP;
    END IF;

    -- Process prefix updates atomically
    FOR _prefix_updates IN
        WITH deleted_with_parents AS (
            SELECT
                d.bucket_id,
                storage.get_direct_parent(d.name) as parent_prefix,
                COUNT(*) as deleted_count
            FROM deleted d
            WHERE position('/' in d.name) > 0  -- Only objects with parent prefixes
            GROUP BY d.bucket_id, storage.get_direct_parent(d.name)
        )
        SELECT bucket_id, parent_prefix, deleted_count
        FROM deleted_with_parents
        WHERE parent_prefix != ''  -- Exclude root level
        ORDER BY bucket_id, parent_prefix  -- Maintain consistent ordering
    LOOP
        PERFORM storage.decrement_prefix_object_count(
            _prefix_updates.bucket_id,
            _prefix_updates.parent_prefix,
            _prefix_updates.deleted_count
        );
    END LOOP;

    RETURN NULL;
END;
$$;

-- Trigger function for object updates (statement-level AFTER)
-- Handles moves between prefixes by updating both old and new parents
CREATE OR REPLACE FUNCTION storage.objects_update_after_ref_counting()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
AS $$
DECLARE
    _all_prefixes text[];
    _prefix_updates RECORD;
BEGIN
    -- Collect all unique prefixes (both old and new) and acquire locks upfront in sorted order to prevent deadlocks
    SELECT array_agg(DISTINCT prefix_name ORDER BY prefix_name)
    INTO _all_prefixes
    FROM (
        SELECT storage.get_direct_parent(o.name) as prefix_name, o.bucket_id
        FROM old_table o
        WHERE position('/' in o.name) > 0
        UNION
        SELECT storage.get_direct_parent(n.name) as prefix_name, n.bucket_id
        FROM new_table n
        WHERE position('/' in n.name) > 0
    ) prefixes
    WHERE prefix_name != '';

    IF _all_prefixes IS NOT NULL AND array_length(_all_prefixes, 1) > 0 THEN
        FOR _prefix_updates IN
            SELECT DISTINCT bucket_id FROM old_table
            UNION
            SELECT DISTINCT bucket_id FROM new_table
        LOOP
            PERFORM storage.lock_multiple_prefixes(_prefix_updates.bucket_id, _all_prefixes);
        END LOOP;
    END IF;

    -- Decrement old parents atomically
    FOR _prefix_updates IN
        WITH old_parents AS (
            SELECT
                o.bucket_id,
                storage.get_direct_parent(o.name) as parent_prefix,
                COUNT(*) as moved_count
            FROM old_table o
            INNER JOIN new_table n ON o.id = n.id
            WHERE position('/' in o.name) > 0
              AND (o.bucket_id != n.bucket_id OR storage.get_direct_parent(o.name) != storage.get_direct_parent(n.name))
            GROUP BY o.bucket_id, storage.get_direct_parent(o.name)
        )
        SELECT bucket_id, parent_prefix, moved_count
        FROM old_parents
        WHERE parent_prefix != ''
        ORDER BY bucket_id, parent_prefix
    LOOP
        PERFORM storage.decrement_prefix_object_count(
            _prefix_updates.bucket_id,
            _prefix_updates.parent_prefix,
            _prefix_updates.moved_count
        );
    END LOOP;

    -- Increment new parents atomically
    FOR _prefix_updates IN
        WITH new_parents AS (
            SELECT
                n.bucket_id,
                storage.get_direct_parent(n.name) as parent_prefix,
                COUNT(*) as moved_count
            FROM new_table n
            INNER JOIN old_table o ON o.id = n.id
            WHERE position('/' in n.name) > 0
              AND (o.bucket_id != n.bucket_id OR storage.get_direct_parent(o.name) != storage.get_direct_parent(n.name))
            GROUP BY n.bucket_id, storage.get_direct_parent(n.name)
        )
        SELECT bucket_id, parent_prefix, moved_count
        FROM new_parents
        WHERE parent_prefix != ''
        ORDER BY bucket_id, parent_prefix
    LOOP
        PERFORM storage.increment_prefix_object_count(
            _prefix_updates.bucket_id,
            _prefix_updates.parent_prefix,
            _prefix_updates.moved_count
        );
    END LOOP;

    RETURN NULL;
END;
$$;

-- Trigger function for prefix insertions
-- Increments parent prefix child_prefixes count
CREATE OR REPLACE FUNCTION storage.prefixes_insert_ref_counting()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM storage.increment_prefix_child_count(NEW.bucket_id, NEW.name);
    RETURN NEW;
END;
$$;

-- Trigger function for prefix deletions
-- Decrements parent prefix child_prefixes count
CREATE OR REPLACE FUNCTION storage.prefixes_delete_ref_counting()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM storage.decrement_prefix_child_count(OLD.bucket_id, OLD.name);
    RETURN OLD;
END;
$$;

-- Object triggers
CREATE TRIGGER objects_insert_set_level
    BEFORE INSERT ON storage.objects
    FOR EACH ROW
    EXECUTE FUNCTION storage.objects_set_level();

CREATE TRIGGER objects_insert_ref_counting
    AFTER INSERT ON storage.objects
    REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT
    EXECUTE FUNCTION storage.objects_insert_after_ref_counting();

CREATE TRIGGER objects_delete_ref_counting
    AFTER DELETE ON storage.objects
    REFERENCING OLD TABLE AS deleted
    FOR EACH STATEMENT
    EXECUTE FUNCTION storage.objects_delete_ref_counting();

CREATE TRIGGER objects_update_set_level
    BEFORE UPDATE ON storage.objects
    FOR EACH ROW
    EXECUTE FUNCTION storage.objects_set_level();

CREATE TRIGGER objects_update_after_ref_counting
    AFTER UPDATE ON storage.objects
    REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
    FOR EACH STATEMENT
    EXECUTE FUNCTION storage.objects_update_after_ref_counting();

-- Prefix triggers
CREATE TRIGGER prefixes_insert_ref_counting
    AFTER INSERT ON storage.prefixes
    FOR EACH ROW
    EXECUTE FUNCTION storage.prefixes_insert_ref_counting();

CREATE TRIGGER prefixes_delete_ref_counting
    AFTER DELETE ON storage.prefixes
    FOR EACH ROW
    EXECUTE FUNCTION storage.prefixes_delete_ref_counting();

-- Create index to help find orphaned prefixes (used by decrement functions and migration 0045 cleanup)
CREATE INDEX IF NOT EXISTS prefixes_empty_idx ON storage.prefixes(bucket_id, level) WHERE child_objects = 0 AND child_prefixes = 0;