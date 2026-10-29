-- Lock buckets before installing triggers and backfilling lifecycle state,
-- so bucket writes cannot race with the backfill. This migration does not
-- touch objects avoiding a lock cycle with object writers.
-- Try briefly and pause between attempts so writers queued behind the lock
-- request wait for one attempt at most while another transaction blocks it.
-- Give up after the caller's lock_timeout, or a minute when unset, so the
-- runner can retry instead of leaving callers waiting indefinitely.
DO $$
DECLARE
    previous_lock_timeout text := current_setting('lock_timeout');
    deadline timestamptz := clock_timestamp() + CASE previous_lock_timeout
        WHEN '0' THEN interval '1 minute'
        ELSE previous_lock_timeout::interval
    END;
BEGIN
    LOOP
        -- PostgreSQL lock_timeout uses milliseconds.
        -- Never set it to zero because that would disable the timeout.
        PERFORM set_config(
            'lock_timeout',
            greatest(1, least(250, floor(extract(epoch FROM deadline - clock_timestamp()) * 1000)))::text || 'ms',
            true
        );
        BEGIN
            LOCK TABLE storage.buckets IN EXCLUSIVE MODE;
            EXIT;
        EXCEPTION WHEN lock_not_available THEN
            IF clock_timestamp() >= deadline THEN
                RAISE;
            END IF;
            PERFORM pg_sleep(greatest(0, least(2, extract(epoch FROM deadline - clock_timestamp()))));
            IF clock_timestamp() >= deadline THEN
                RAISE;
            END IF;
        END;
    END LOOP;
    PERFORM set_config('lock_timeout', previous_lock_timeout, true);
END;
$$;

-- Older builds rely on the foreign key to delete lifecycle state with the bucket.
-- Allow this only when there is no in-flight batch and the continuation is understood.
-- The check does not change state so a later trigger can still cancel the deletion.
CREATE OR REPLACE FUNCTION storage.protect_active_lifecycle_states()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM storage.bucket_lifecycle_states
        WHERE bucket_id = OLD.id
          AND NOT (
              continuation IS NULL
              OR (continuation @> '{"continuationVersion": 1}' AND continuation #> '{batch,inFlight}' IS NULL)
          )
    ) THEN
        RAISE EXCEPTION 'update or delete on table "buckets" violates foreign key constraint "bucket_lifecycle_states_bucket_id_fkey" on table "bucket_lifecycle_states"'
            USING ERRCODE = 'foreign_key_violation',
                  DETAIL = format('Key (id)=(%s) is still referenced from table "bucket_lifecycle_states".', OLD.id),
                  SCHEMA = 'storage',
                  TABLE = 'bucket_lifecycle_states',
                  CONSTRAINT = 'bucket_lifecycle_states_bucket_id_fkey';
    END IF;
    RETURN OLD;
END;
$$;

REVOKE ALL ON FUNCTION storage.protect_active_lifecycle_states() FROM PUBLIC;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_trigger
        WHERE tgrelid = 'storage.buckets'::regclass AND tgname = 'protect_active_lifecycle_states'
    ) THEN
        CREATE TRIGGER protect_active_lifecycle_states
        BEFORE DELETE ON storage.buckets
        FOR EACH ROW
        EXECUTE FUNCTION storage.protect_active_lifecycle_states();
    END IF;
END;
$$;

-- Policy changes reset pending work but an in-flight batch must finish
-- recovery before its state can be cleared.
CREATE OR REPLACE FUNCTION storage.reconcile_lifecycle_state(target_bucket_id text, policy_changed boolean)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    bucket storage.buckets%ROWTYPE;
    active boolean;
    idle boolean;
    state_generation uuid;
BEGIN
    SELECT * INTO bucket FROM storage.buckets WHERE id = target_bucket_id FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT continuation IS NULL OR (
               continuation @> '{"continuationVersion": 1}'
               AND continuation #> '{batch,inFlight}' IS NULL
           ), generation
    INTO idle, state_generation
    FROM storage.bucket_lifecycle_states
    WHERE bucket_id = target_bucket_id
    FOR UPDATE;

    IF bucket.lifecycle_configuration IS NULL THEN
        DELETE FROM storage.bucket_lifecycle_states
        WHERE bucket_id = target_bucket_id
          AND idle IS TRUE;

        UPDATE storage.bucket_lifecycle_states
        SET next_run_at = clock_timestamp(), updated_at = clock_timestamp()
        WHERE bucket_id = target_bucket_id
          AND idle IS NOT TRUE;
        RETURN;
    END IF;

    active := bucket.versioning_status <> 'DISABLED'
        AND bucket.lifecycle_configuration @> '{"rules":[{"status":"Enabled"}]}'::jsonb;

    -- The state row records the generation it was reconciled with so callers need not detect
    -- policy changes. Rows without a recorded generation keep their work.
    policy_changed := policy_changed OR (
        state_generation IS NOT NULL AND state_generation <> bucket.lifecycle_configuration_generation
    );

    INSERT INTO storage.bucket_lifecycle_states AS state (bucket_id, generation, next_run_at)
    VALUES (bucket.id, bucket.lifecycle_configuration_generation, CASE WHEN active THEN clock_timestamp() END)
    ON CONFLICT (bucket_id) DO UPDATE
    SET next_run_at = CASE
            WHEN policy_changed THEN
                CASE WHEN active OR idle IS NOT TRUE THEN clock_timestamp() END
            ELSE EXCLUDED.next_run_at
        END,
        generation = EXCLUDED.generation,
        claim_id = CASE WHEN policy_changed AND idle IS TRUE
                        THEN NULL ELSE state.claim_id END,
        claim_until = CASE WHEN policy_changed AND idle IS TRUE
                           THEN NULL ELSE state.claim_until END,
        continuation = CASE WHEN policy_changed AND idle IS TRUE
                            THEN NULL ELSE state.continuation END,
        failure_count = CASE WHEN policy_changed AND idle IS TRUE
                             THEN 0 ELSE state.failure_count END,
        last_error = CASE WHEN policy_changed AND idle IS TRUE
                          THEN NULL ELSE state.last_error END,
        updated_at = clock_timestamp()
    WHERE policy_changed OR (state.next_run_at IS NULL AND EXCLUDED.next_run_at IS NOT NULL);
END;
$$;

REVOKE ALL ON FUNCTION storage.reconcile_lifecycle_state(text, boolean) FROM PUBLIC;

DO $$
DECLARE
    role_name text;
BEGIN
    FOR role_name IN
        SELECT DISTINCT acl.grantee::regrole::text
        FROM pg_catalog.pg_proc AS proc, pg_catalog.aclexplode(proc.proacl) AS acl
        WHERE proc.oid = 'storage.reconcile_lifecycle_state(text, boolean)'::regprocedure
          AND acl.grantee NOT IN (0, proc.proowner)
    LOOP
        EXECUTE format('REVOKE ALL ON FUNCTION storage.reconcile_lifecycle_state(text, boolean) FROM %s', role_name);
    END LOOP;

    FOREACH role_name IN ARRAY ARRAY[
        'postgres',
        COALESCE(current_setting('storage.super_user', true), 'postgres'),
        COALESCE(current_setting('storage.service_role', true), 'service_role')
    ] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('GRANT EXECUTE ON FUNCTION storage.reconcile_lifecycle_state(text, boolean) TO %I', role_name);
        END IF;
    END LOOP;
END;
$$;

-- OrioleDB can give AFTER triggers identical OLD/NEW images for a changed row so waking never compares them.
CREATE OR REPLACE FUNCTION storage.wake_lifecycle_state()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.lifecycle_configuration IS NULL THEN
        RETURN NEW;
    END IF;

    PERFORM storage.reconcile_lifecycle_state(NEW.id, false);
    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION storage.wake_lifecycle_state() FROM PUBLIC;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_trigger
        WHERE tgrelid = 'storage.buckets'::regclass AND tgname = 'wake_lifecycle_on_versioning_enable'
    ) THEN
        CREATE TRIGGER wake_lifecycle_on_versioning_enable
        AFTER UPDATE OF versioning_status ON storage.buckets
        FOR EACH ROW
        WHEN (NEW.lifecycle_configuration IS NOT NULL)
        EXECUTE FUNCTION storage.wake_lifecycle_state();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_trigger
        WHERE tgrelid = 'storage.buckets'::regclass AND tgname = 'wake_lifecycle_on_policy_write'
    ) THEN
        CREATE TRIGGER wake_lifecycle_on_policy_write
        AFTER INSERT OR UPDATE OF lifecycle_configuration_generation ON storage.buckets
        FOR EACH ROW
        EXECUTE FUNCTION storage.wake_lifecycle_state();
    END IF;
END;
$$;

-- Delete metadata and record deleted IDs in the same transaction before
-- deleting backend bytes. Blocked rows keep both their metadata and bytes.
-- Deleting metadata also prevents new foreign-key references during recovery.
CREATE OR REPLACE FUNCTION storage.delete_lifecycle_objects(object_ids uuid[])
RETURNS TABLE(id uuid, is_delete_marker boolean, metadata jsonb)
LANGUAGE plpgsql
AS $$
DECLARE
    object_id uuid;
    deleted_row storage.objects%ROWTYPE;
    deleted_ids uuid[];
    deleted_markers boolean[];
    deleted_metadata jsonb[];
    bulk_deleted boolean := false;
BEGIN
    -- Surface pre-existing deferred violations instead of treating them as blocked rows.
    SET CONSTRAINTS ALL IMMEDIATE;
    PERFORM set_config('storage.allow_delete_query', 'true', true);

    -- One subxact keeps the subxid cache from overflowing; rows return only once it commits.
    BEGIN
        WITH deleted AS (
            DELETE FROM storage.objects AS object_row
              WHERE object_row.id = ANY(object_ids) AND object_row.archived_at IS NOT NULL
              RETURNING object_row.id, object_row.is_delete_marker, object_row.metadata
        )
        SELECT array_agg(deleted.id), array_agg(deleted.is_delete_marker), array_agg(deleted.metadata)
          INTO deleted_ids, deleted_markers, deleted_metadata
          FROM deleted;
        bulk_deleted := true;
    EXCEPTION
        WHEN OTHERS THEN
            -- User triggers may use insufficient_privilege to block a row.
            IF SQLSTATE <> '42501' AND left(SQLSTATE, 2) IN ('08', '40', '42', '53', '55', '57', '58', 'XX') THEN
                RAISE;
            END IF;
    END;
    IF bulk_deleted THEN
        RETURN QUERY SELECT * FROM unnest(deleted_ids, deleted_markers, deleted_metadata);
        RETURN;
    END IF;

    FOREACH object_id IN ARRAY object_ids LOOP
        BEGIN
            DELETE FROM storage.objects AS object_row
              WHERE object_row.id = object_id AND object_row.archived_at IS NOT NULL
              RETURNING object_row.* INTO deleted_row;
            IF FOUND THEN
                id := deleted_row.id;
                is_delete_marker := deleted_row.is_delete_marker;
                metadata := deleted_row.metadata;
                RETURN NEXT;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLSTATE <> '42501' AND left(SQLSTATE, 2) IN ('08', '40', '42', '53', '55', '57', '58', 'XX') THEN
                    RAISE;
                END IF;
        END;
    END LOOP;
END;
$$;

REVOKE ALL ON FUNCTION storage.delete_lifecycle_objects(uuid[]) FROM PUBLIC;

DO $$
DECLARE
    role_name text;
BEGIN
    FOR role_name IN
        SELECT DISTINCT acl.grantee::regrole::text
        FROM pg_catalog.pg_proc AS proc, pg_catalog.aclexplode(proc.proacl) AS acl
        WHERE proc.oid = 'storage.delete_lifecycle_objects(uuid[])'::regprocedure
          AND acl.grantee NOT IN (0, proc.proowner)
    LOOP
        EXECUTE format('REVOKE ALL ON FUNCTION storage.delete_lifecycle_objects(uuid[]) FROM %s', role_name);
    END LOOP;

    FOREACH role_name IN ARRAY ARRAY[
        'postgres',
        COALESCE(current_setting('storage.super_user', true), 'postgres'),
        COALESCE(current_setting('storage.service_role', true), 'service_role')
    ] LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('GRANT EXECUTE ON FUNCTION storage.delete_lifecycle_objects(uuid[]) TO %I', role_name);
        END IF;
    END LOOP;
END;
$$;

CREATE TABLE IF NOT EXISTS storage.lifecycle_abandoned_artifacts (
    bucket_id text NOT NULL,
    key text NOT NULL,
    name text NOT NULL,
    version text,
    attempted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket_id, key)
);

DO $$
BEGIN
    IF NOT (SELECT relrowsecurity FROM pg_catalog.pg_class
            WHERE oid = 'storage.lifecycle_abandoned_artifacts'::regclass) THEN
        ALTER TABLE storage.lifecycle_abandoned_artifacts ENABLE ROW LEVEL SECURITY;
    END IF;
    IF to_regclass('storage.lifecycle_abandoned_artifacts_attempted_at_idx') IS NULL THEN
        CREATE INDEX IF NOT EXISTS lifecycle_abandoned_artifacts_attempted_at_idx
          ON storage.lifecycle_abandoned_artifacts (attempted_at);
    END IF;
END;
$$;

DO $$
DECLARE
    anon_role text = COALESCE(current_setting('storage.anon_role', true), 'anon');
    authenticated_role text = COALESCE(current_setting('storage.authenticated_role', true), 'authenticated');
    service_role text = COALESCE(current_setting('storage.service_role', true), 'service_role');
    super_user text = COALESCE(current_setting('storage.super_user', true), 'postgres');
    role_name text;
BEGIN
    REVOKE ALL ON TABLE storage.lifecycle_abandoned_artifacts FROM PUBLIC;

    FOREACH role_name IN ARRAY ARRAY[anon_role, authenticated_role]
    LOOP
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
        EXECUTE format('REVOKE ALL ON TABLE storage.lifecycle_abandoned_artifacts FROM %I', role_name);
      END IF;
    END LOOP;

    FOREACH role_name IN ARRAY ARRAY['postgres', super_user, service_role]
    LOOP
      IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = role_name) THEN
        EXECUTE format('GRANT ALL ON TABLE storage.lifecycle_abandoned_artifacts TO %I', role_name);
      END IF;
    END LOOP;
END;
$$;

-- Repair missing/dormant state without resetting existing schedules or journals.
SELECT storage.reconcile_lifecycle_state(id, false)
FROM storage.buckets
WHERE lifecycle_configuration IS NOT NULL;
