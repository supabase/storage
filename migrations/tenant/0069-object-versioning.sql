-- Unlocks real versioning: only run once every supporting index (0064-0069) exists
ALTER TABLE storage.buckets DROP CONSTRAINT IF EXISTS buckets_versioning_dark_check;
