ALTER TABLE tenants ADD COLUMN IF NOT EXISTS database_pool_url text DEFAULT NULL;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS max_connections int DEFAULT NULL;
