-- Disable the objects delete trigger to prevent race conditions
-- Prefix cleanup is now handled at the application level with proper locking
-- Drop the trigger that causes race conditions during concurrent deletions
DROP TRIGGER IF EXISTS "objects_delete_delete_prefix" ON "storage"."objects";
