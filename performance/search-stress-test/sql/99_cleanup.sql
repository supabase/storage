-- Removes everything this harness creates in storage.objects/buckets. Does NOT drop
-- the perf schema (results/dataset_meta) by default so past run history survives -
-- pass -v drop_perf_schema=true to also drop perf.* entirely.

\set ON_ERROR_STOP on
\i _defaults.sql
\if :{?drop_perf_schema}
\else
  \set drop_perf_schema false
\endif

SET storage.allow_delete_query = 'true';
DELETE FROM storage.objects WHERE bucket_id = :'bucket_id';
DELETE FROM storage.buckets WHERE id = :'bucket_id';

\if :drop_perf_schema
DROP SCHEMA perf CASCADE;
\echo Dropped bucket :bucket_id and the perf schema.
\else
\echo Dropped bucket :bucket_id. perf schema (results/dataset_meta) left intact - rerun with -v drop_perf_schema=true to remove it too.
\endif
