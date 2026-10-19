-- Existing policies, restored tenants and freshly migrated tenant DBs need dispatch without another policy PUT.
CREATE OR REPLACE FUNCTION public.enroll_lifecycle_tenant()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
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
