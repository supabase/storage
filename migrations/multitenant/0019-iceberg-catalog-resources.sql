CREATE TABLE IF NOT EXISTS iceberg_namespaces (
  bucket_id text NOT NULL,
  name text COLLATE "C" NOT NULL,
  tenant_id text NOT NULL,
  created_at timestamptz NOT NULL default now(),
  updated_at timestamptz NOT NULL default now(),
  primary key (tenant_id, bucket_id, name)
);