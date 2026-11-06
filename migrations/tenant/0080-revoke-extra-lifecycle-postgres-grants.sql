-- Retain read access for dumps while removing the extra write and execute
-- grants, unless postgres is a configured role or owns the object.
DO $$
DECLARE
    postgres_oid oid;
    super_user text = COALESCE(current_setting('storage.super_user', true), 'postgres');
    service_role text = COALESCE(current_setting('storage.service_role', true), 'service_role');
BEGIN
    SELECT oid INTO postgres_oid FROM pg_catalog.pg_roles WHERE rolname = 'postgres';
    IF postgres_oid IS NULL OR 'postgres' IN (super_user, service_role) THEN
        RETURN;
    END IF;

    IF (SELECT relowner FROM pg_catalog.pg_class WHERE oid = 'storage.bucket_lifecycle_states'::regclass) <> postgres_oid THEN
        REVOKE ALL ON TABLE storage.bucket_lifecycle_states FROM postgres;
        GRANT SELECT ON TABLE storage.bucket_lifecycle_states TO postgres;
    END IF;
    IF (SELECT relowner FROM pg_catalog.pg_class WHERE oid = 'storage.lifecycle_abandoned_artifacts'::regclass) <> postgres_oid THEN
        REVOKE ALL ON TABLE storage.lifecycle_abandoned_artifacts FROM postgres;
        GRANT SELECT ON TABLE storage.lifecycle_abandoned_artifacts TO postgres;
    END IF;
    IF (SELECT proowner FROM pg_catalog.pg_proc WHERE oid = 'storage.reconcile_lifecycle_state(text, boolean)'::regprocedure) <> postgres_oid THEN
        REVOKE ALL ON FUNCTION storage.reconcile_lifecycle_state(text, boolean) FROM postgres;
    END IF;
    IF (SELECT proowner FROM pg_catalog.pg_proc WHERE oid = 'storage.delete_lifecycle_objects(uuid[])'::regprocedure) <> postgres_oid THEN
        REVOKE ALL ON FUNCTION storage.delete_lifecycle_objects(uuid[]) FROM postgres;
    END IF;
END;
$$;
