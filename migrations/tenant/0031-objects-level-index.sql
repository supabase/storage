-- postgres-migrations disable-transaction
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "objects_bucket_id_level_idx"
    ON "storage"."objects" ("bucket_id", level, "name" COLLATE "C");
