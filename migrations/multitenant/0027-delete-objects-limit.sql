ALTER TABLE tenants ADD COLUMN IF NOT EXISTS delete_objects_limit int NULL;

ALTER TABLE tenants
  DROP CONSTRAINT IF EXISTS tenants_delete_objects_limit_positive;

ALTER TABLE tenants
  ADD CONSTRAINT tenants_delete_objects_limit_positive
  CHECK (delete_objects_limit IS NULL OR delete_objects_limit > 0);
