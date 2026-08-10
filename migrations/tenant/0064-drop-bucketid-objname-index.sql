-- postgres-migrations disable-transaction
-- bucketid_objname predates versioning and can't coexist with multiple rows per key
DROP INDEX CONCURRENTLY IF EXISTS storage.bucketid_objname;
