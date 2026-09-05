ALTER TABLE tenants ADD COLUMN IF NOT EXISTS feature_object_versioning boolean NOT NULL DEFAULT false;
