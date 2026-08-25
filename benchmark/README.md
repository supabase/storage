# Object versioning — wave-2 performance benchmark

Answers one question: did rewriting `list_objects_with_delimiter`, `search`,
`search_by_timestamp`, `search_v2`, and `get_size_by_bucket` for object
versioning (wave-2) regress their performance?

## Methodology

All migration files (pre- and post-`object-versioning-core`) already coexist
in `migrations/tenant/` on this branch. `DB_MIGRATIONS_FREEZE_AT` stops the
migration runner at a named migration, so switching between the PRE and POST
schema is just re-running migrations with/without that env var - no branch
checkout needed.

The benchmark runner (`run-benchmark.ts`) calls all 5 functions directly via
named-parameter SQL, not through `pg.ts`'s app-level methods. That's
deliberate: the app layer routes delimiter-based listing through
`storage.search_v2()` whenever the `search-v2` migration is present, which is
true in *both* PRE and POST states here (`search-v2` is a much older migration
than `object-versioning-core`) - going through the app layer would make
`list_objects_with_delimiter` effectively untested. Calling each function
directly guarantees all 5 actually get exercised.

The pre- and post-migration signatures share identical leading parameter
names for all 4 non-aggregate functions - wave-2 only appended new trailing
params with defaults (confirmed against `tyler/chore/function-diffs-before`).
So the same scenario definitions in `scenarios.ts` work unmodified against
either schema state: version-aware scenarios (`noncurrentVersions`,
`deleteMarkers`) are simply skipped when running against the PRE dataset,
since those parameters don't exist there.

For each scenario: 1 discarded warmup call, then 10 timed iterations
(wall-clock via `performance.now()`, matching what the app actually
experiences), plus one `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` call for
planning time and buffer stats.

## Datasets

- **PRE** (10M rows / 10M unique keys): flat, pre-versioning schema shape.
- **POST — default** (10M rows / 10M unique keys): full wave-2 schema, but
  every key is single-version (`is_versioned = false`, `archived_at IS NULL`,
  no delete markers) - simulates a bucket that's never touched versioning, at
  the same scale as PRE. This is the one that must show no regression.
- **POST — versioned** (~10M total rows, long-tail distribution): ~90% of
  keys have 1 version, ~9.5% have 2-5 versions, ~0.5% have 20-50 versions
  (with ~10% of that deep-history bucket ending in a delete marker). Exercises
  the tri-state filters and multi-row pagination with real history.

All three datasets share the same key/folder naming scheme
(`folder-NNNNN/key-NNNNNN.bin`, 1000 keys per folder) so query shapes are
directly comparable across runs.

## Running it

```bash
# 1. PRE baseline
docker compose --project-directory . --profile monitoring -f ./.docker/docker-compose-infra.yml down -v
docker compose --project-directory . --profile monitoring -f ./.docker/docker-compose-infra.yml up -d
DB_MIGRATIONS_FREEZE_AT=mark-filename-immutable npm run migration:run
cat benchmark/seed/seed-pre.sql | docker exec -i storage-versioning-tenant_db-1 psql -U postgres -d postgres
DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:5432/postgres" npx tsx benchmark/run-benchmark.ts --label=pre --state=pre

# 2. POST — default path (fresh DB, full migrations)
docker compose --project-directory . --profile monitoring -f ./.docker/docker-compose-infra.yml down -v
npm run infra:start
cat benchmark/seed/seed-post-default.sql | docker exec -i storage-versioning-tenant_db-1 psql -U postgres -d postgres
DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:5432/postgres" npx tsx benchmark/run-benchmark.ts --label=post-default --state=post-default

# 3. POST — heavy-version path (same fully-migrated DB, reset objects table first)
docker exec -i storage-versioning-tenant_db-1 psql -U postgres -d postgres -c "SET storage.allow_delete_query = true; DELETE FROM storage.objects WHERE bucket_id = 'benchmark'; DELETE FROM storage.buckets WHERE id = 'benchmark';"
cat benchmark/seed/seed-post-versioned.sql | docker exec -i storage-versioning-tenant_db-1 psql -U postgres -d postgres
DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:5432/postgres" npx tsx benchmark/run-benchmark.ts --label=post-versioned --state=post-versioned
```

Override scale for a fast sanity check before committing to a full 10M-row
run: append `-v target_rows=100000 -v keys_per_folder=100` to the seed
`psql` calls (both must change together, or the folder distribution skews).

Results land in `benchmark/results/<label>-<timestamp>.json`. After all three
runs, hand-update `benchmark/results/SUMMARY.md` with a comparison table -
that's the artifact meant for PR review, not the raw JSON.

## Sanity-checking a seed

Each seed script prints its own row-count summary at the end. To re-check
later:

```sql
SELECT count(*) AS total_rows,
       count(DISTINCT name) AS unique_keys,
       count(*) FILTER (WHERE archived_at IS NOT NULL) AS archived_rows,
       count(*) FILTER (WHERE is_delete_marker) AS delete_marker_rows
FROM storage.objects WHERE bucket_id = 'benchmark';
```
