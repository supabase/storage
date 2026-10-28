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

INSERT INTO public.lifecycle_tenants (tenant_id, next_dispatch_at)
SELECT id, clock_timestamp() FROM public.tenants WHERE feature_object_versioning
ON CONFLICT (tenant_id) DO UPDATE
SET next_dispatch_at = LEAST(lifecycle_tenants.next_dispatch_at, EXCLUDED.next_dispatch_at);
