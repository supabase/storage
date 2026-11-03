-- postgres-migrations disable-transaction
-- Run as a top-level DO statement: commit DDL before backfilling tenants.
DO $migration$
DECLARE
    attempt integer;
    succeeded boolean;
    last_id text;
    after_id text := NULL;
    include_cursor boolean := true;
    next_id text;
BEGIN
    FOR attempt IN 1..5 LOOP
        BEGIN
            SET LOCAL lock_timeout = '250ms';
            CREATE TABLE IF NOT EXISTS public.lifecycle_tenants (
                tenant_id text PRIMARY KEY
                  REFERENCES public.tenants(id)
                  ON DELETE CASCADE,

                next_dispatch_at timestamptz NOT NULL DEFAULT now(),

                claim_id uuid,
                claim_until timestamptz,

                last_dispatched_at timestamptz,
                last_completed_at timestamptz,
                last_success_at timestamptz,

                failure_count integer NOT NULL DEFAULT 0,
                last_error jsonb,

                -- Advisory snapshots collected by tenant dispatchers.
                -- Timestamps expose stale or unavailable tenant observations.
                -- Scheduling continues to use the tenant state.
                max_bucket_failure_count integer,
                oldest_in_flight_started_at timestamptz,
                bucket_status_observed_at timestamptz,

                CONSTRAINT lifecycle_tenants_failure_count_check
                  CHECK (failure_count >= 0)
            );

            CREATE INDEX IF NOT EXISTS lifecycle_tenants_due_idx
            ON public.lifecycle_tenants (
                next_dispatch_at,
                tenant_id
            );

            CREATE INDEX IF NOT EXISTS lifecycle_tenants_expired_claim_idx
            ON public.lifecycle_tenants (
                claim_until,
                tenant_id
            )
            WHERE claim_until IS NOT NULL;

            -- Existing policies, restored tenants and freshly migrated tenant DBs need dispatch without another policy PUT.
            CREATE OR REPLACE FUNCTION public.enroll_lifecycle_tenant()
            RETURNS trigger
            LANGUAGE plpgsql
            SET search_path = pg_catalog
            AS $$
            BEGIN
                IF TG_OP = 'UPDATE'
                    AND OLD.feature_object_versioning IS NOT DISTINCT FROM NEW.feature_object_versioning
                    AND OLD.migrations_version IS NOT DISTINCT FROM NEW.migrations_version THEN
                    RETURN NEW;
                END IF;
                INSERT INTO public.lifecycle_tenants (tenant_id, next_dispatch_at)
                VALUES (NEW.id, clock_timestamp())
                ON CONFLICT (tenant_id) DO UPDATE
                SET next_dispatch_at = LEAST(lifecycle_tenants.next_dispatch_at, EXCLUDED.next_dispatch_at);
                RETURN NEW;
            END;
            $$;

            DROP TRIGGER IF EXISTS enroll_lifecycle_tenant ON public.tenants;
            CREATE TRIGGER enroll_lifecycle_tenant
            AFTER INSERT OR UPDATE OF feature_object_versioning, migrations_version ON public.tenants
            FOR EACH ROW WHEN (NEW.feature_object_versioning)
            EXECUTE FUNCTION public.enroll_lifecycle_tenant();

            -- Bound the scan while the schema is installed. After commit, the
            -- trigger covers inserts and feature activation on either side of the cursor.
            SELECT id INTO last_id FROM public.tenants ORDER BY id DESC LIMIT 1;
            SELECT id INTO after_id FROM public.tenants ORDER BY id LIMIT 1;
            succeeded := true;
        EXCEPTION WHEN lock_not_available OR deadlock_detected THEN
            IF attempt = 5 THEN
                RAISE;
            END IF;
            succeeded := false;
        END;

        -- Transaction control must be outside the exception block. Rollback
        -- releases all acquired locks before waiting for the next attempt.
        IF succeeded THEN
            COMMIT;
            EXIT;
        END IF;
        ROLLBACK;
        RAISE NOTICE 'Retrying lifecycle tenant schema after lock conflict (attempt %/5)', attempt;
        PERFORM pg_sleep(0.25 * attempt);
    END LOOP;

    IF last_id IS NULL THEN
        RETURN;
    END IF;

    LOOP
        FOR attempt IN 1..5 LOOP
            BEGIN
                SET LOCAL lock_timeout = '250ms';
                WITH batch AS MATERIALIZED (
                    SELECT id, feature_object_versioning
                    FROM public.tenants
                    -- Keep an indexable lower bound even with a cached generic plan.
                    WHERE id >= after_id AND id <= last_id
                      AND (include_cursor OR id > after_id)
                    ORDER BY id
                    LIMIT 1000
                    -- Protect the FK check against concurrent deletion. These
                    -- row locks allow ordinary non-key tenant updates.
                    FOR KEY SHARE
                ), enrolled AS (
                    INSERT INTO public.lifecycle_tenants (tenant_id, next_dispatch_at)
                    SELECT id, clock_timestamp() FROM batch WHERE feature_object_versioning
                    ON CONFLICT (tenant_id) DO UPDATE
                    SET next_dispatch_at = LEAST(lifecycle_tenants.next_dispatch_at, EXCLUDED.next_dispatch_at)
                )
                SELECT id INTO next_id FROM batch ORDER BY id DESC LIMIT 1;
                succeeded := true;
            EXCEPTION WHEN lock_not_available OR deadlock_detected THEN
                IF attempt = 5 THEN
                    RAISE;
                END IF;
                succeeded := false;
            END;

            IF succeeded THEN
                COMMIT;
                EXIT;
            END IF;
            ROLLBACK;
            RAISE NOTICE 'Retrying lifecycle tenant backfill after lock conflict (attempt %/5)', attempt;
            PERFORM pg_sleep(0.25 * attempt);
        END LOOP;

        EXIT WHEN next_id IS NULL OR next_id = last_id;
        after_id := next_id;
        include_cursor := false;
    END LOOP;
END;
$migration$;
