CREATE TABLE IF NOT EXISTS storage.tenants (
  id text PRIMARY KEY,
  config jsonb NOT NULL
);
