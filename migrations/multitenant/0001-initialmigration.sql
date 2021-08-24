CREATE TABLE IF NOT EXISTS tenants (
  id text PRIMARY KEY,
  config jsonb NOT NULL
);
