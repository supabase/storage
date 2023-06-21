create extension if not exists "uuid-ossp";

CREATE TABLE tenants_external_credentials (
    "id" uuid NOT NULL DEFAULT uuid_generate_v4(),
    name text NOT NULL unique,
    tenant_id text NOT NULL references tenants (id),
    provider text NOT NULL default 's3',
    access_key text NULL,
    secret_key text NULL,
    role text null,
    region text not null,
    endpoint text NULL,
    force_path_style boolean NOT NULL default false,
    PRIMARY KEY (id)
);

create index external_buckets_tenant_id_idx on tenants_external_credentials (tenant_id);

CREATE FUNCTION tenants_external_credentials_update_notify_trigger ()
    RETURNS TRIGGER
AS $$
BEGIN
    PERFORM
        pg_notify('tenants_external_credentials_update', '"' || NEW.id || ':' || NEW.tenant_id || '"');
    RETURN NULL;
END;
$$
    LANGUAGE plpgsql;

CREATE TRIGGER tenants_external_credentials_notify_trigger
    AFTER DELETE ON tenants_external_credentials
    FOR EACH ROW
EXECUTE PROCEDURE tenants_external_credentials_update_notify_trigger();

CREATE TRIGGER tenants_external_credentials_notify_trigger
    AFTER UPDATE ON tenants_external_credentials
    FOR EACH ROW
EXECUTE PROCEDURE tenants_external_credentials_update_notify_trigger();