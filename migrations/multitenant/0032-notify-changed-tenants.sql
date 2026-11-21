-- Preserve the existing channel and payload for old and new listeners.
-- A no-op UPDATE should not invalidate caches or serialize notification commits.
CREATE OR REPLACE FUNCTION public.tenants_update_notify_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD *<> NEW THEN
        PERFORM pg_notify('tenants_update', '"' || NEW.id || '"');
    END IF;
    RETURN NULL;
END;
$$;
