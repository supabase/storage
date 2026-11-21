CREATE OR REPLACE FUNCTION public.set_tenant_updated_at()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    -- Match the notification trigger's binary row comparison, including fields
    -- without equality operators. A no-op must not change the timestamp or notify.
    IF OLD *<> NEW THEN
        -- Keep the timestamp increasing even if the clock moves backward.
        NEW.updated_at := GREATEST(clock_timestamp(), OLD.updated_at + interval '1 microsecond');
    END IF;
    RETURN NEW;
END;
$$;

ALTER TABLE public.tenants
ADD COLUMN IF NOT EXISTS updated_at timestamptz;

ALTER TABLE public.tenants
ALTER COLUMN updated_at
SET DEFAULT clock_timestamp();

DROP TRIGGER IF EXISTS set_tenant_updated_at ON public.tenants;

CREATE TRIGGER set_tenant_updated_at
    BEFORE UPDATE ON public.tenants
    FOR EACH ROW
    EXECUTE FUNCTION public.set_tenant_updated_at();
