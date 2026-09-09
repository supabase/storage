# storage.search() stress test

Performance harness for `storage.search` (the list-v1 DB function; see
`migrations/tenant/0063-fix-search-name-relative-to-prefix.sql`). Seeds a large
synthetic dataset, times `storage.search()` across sort column x sort direction x
offset depth, with and without case-colliding folders, and can EXPLAIN ANALYZE the
same scenarios at small and large scale to look for planner/index issues.

This is a standalone dev tool - it isn't wired into `npm test` and doesn't touch
application code. It talks directly to Postgres via `psql`.

## Layout

```
run.sh              orchestrator - parses flags, runs the .sql files below in order
sql/
  _defaults.sql      shared psql variable defaults, \i'd from every other file
  00_schema.sql      perf.* bookkeeping tables + helper function/procedure (idempotent)
  01_seed.sql        seeds stress/ (clean) and stress_collision/ (with collisions)
  10_bench_root.sql  times root-level folder listing across the offset/sort matrix
  11_bench_within_folder.sql  times listing inside one large folder
  20_explain.sql     EXPLAIN (ANALYZE, BUFFERS) for representative scenarios
  90_export_results.sql  streams one run's timings out as CSV
  99_cleanup.sql     deletes the seeded bucket (optionally the perf.* bookkeeping too)
results/             timestamped .csv / .txt output lands here (gitignored)
```

Every `sql/*.sql` file runs standalone with sane defaults (e.g.
`psql "$DB_URL" -f sql/01_seed.sql`) as well as through `run.sh`.

## Quick start

```bash
# 1. Smoke test - confirms everything works end to end in seconds
./run.sh --rows 1000

# 2. A meaningfully large run
./run.sh --rows 1000000

# 3. Real stress testing. This seeds 100M+ rows across two datasets - it WILL take a
#    while (expect double-digit minutes depending on hardware/disk) and use several
#    GB of disk. Only run this against a local/dev database you don't mind loading up.
./run.sh --rows 100000000
```

Each run prints a live summary (avg ms per scenario/sort/offset) and writes:
- `results/run-<run_id>-<label>-rows<N>.csv` - every individual timed call
- `results/explain-<run_id>-<label>-rows<N>.txt` - EXPLAIN output (only with `--phase explain`)

## What gets seeded

Two parallel datasets share one bucket (`perf-stress-test` by default):

- `stress/folder_NNNNNNN/file_NNNNNN.dat` - `folder_count` folders x
  `files_per_folder` files, no case collisions anywhere. `folder_count` is derived
  from `--rows` (clamped to [10, 20000]) so root-level listing (many sibling
  folders - exercises the folder skip-scan) and within-folder listing (many files -
  exercises the file batch-fetch loop) are both meaningfully sized regardless of
  scale.
- `stress_collision/` - the same folders, plus a case-flipped twin
  (`FOLDER_NNNNNNN` vs `folder_NNNNNNN`) for `collision_rate` (default 5%) of them.
  **Twins get the exact same child file names as the original.** That's
  deliberate: two case-variant folders whose children share a relative name is the
  pathological case the collision fix specifically had to handle (see the comments
  in `0063-fix-search-name-relative-to-prefix.sql` - two folders that tie exactly on
  `lower(name)`), so it's the case worth stress-testing, not the easy case.

## What gets benchmarked

For each of the clean and collision datasets, at a shallow (offset 0), mid-depth,
and near-the-end offset, for `sort_column` in `{name, created_at}` and `sort_order`
in `{asc, desc}`: one `storage.search()` call, timed with `clock_timestamp()` inside
the DB (not psql's `\timing`, so it's independent of how the call is invoked) and
logged to `perf.search_perf_results`.

`created_at` stands in for all three non-name sort columns (`updated_at`,
`last_accessed_at` take the identical code path - the ILIKE + `GROUP BY` branch -
so timing more than one of them doesn't add information).

Offsets matter here specifically because this function paginates via `OFFSET`
semantics (walking and discarding `offset` rows every call, not a keyset cursor) -
that's where cost-scales-with-depth problems would show up.

## Testing against older function versions

`--function <path>` applies that `.sql` file's `CREATE OR REPLACE FUNCTION
storage.search(...)` before the bench/explain phases run, and tags results with
`--label` (defaults to the file's basename). **This mutates the live function on
whatever `--db-url` points at - only use a local/dev database.**

Seed once, then bench repeatedly against different versions without re-seeding
(seeding 100M+ rows is the expensive part):

```bash
./run.sh --rows 1000000 --phase schema,seed          # seed once

# extract the historical versions worth comparing
git show b5b782f5694:migrations/tenant/0050-search-v2-optimised.sql               > /tmp/0050-original.sql
git show e65aefe1106:migrations/tenant/0056-fix-optimized-search-function.sql     > /tmp/0056-partial-fix.sql
git show 8aa704af5c7:migrations/tenant/0063-fix-search-name-relative-to-prefix.sql > /tmp/0063-pre-collision-fix.sql
# current fixed version is just the file already in the repo

./run.sh --rows 1000000 --phase bench,explain,export --function /tmp/0050-original.sql              --label 0050-original          --run-id compare1
./run.sh --rows 1000000 --phase bench,explain,export --function /tmp/0056-partial-fix.sql            --label 0056-partial-fix       --run-id compare1
./run.sh --rows 1000000 --phase bench,explain,export --function /tmp/0063-pre-collision-fix.sql      --label 0063-pre-collision-fix --run-id compare1
./run.sh --rows 1000000 --phase bench,explain,export --function ../../migrations/tenant/0063-fix-search-name-relative-to-prefix.sql --label 0063-current --run-id compare1

# restore the real current version when done (see below)
```

Passing the same `--run-id` across calls means all four end up in the same
`results/run-compare1-*.csv` scenario space (still one CSV per label/run, but
sharing a `run_id` column) so they're easy to diff/join.

Note: `0050-original` and `0056-partial-fix` will effectively hang or return wrong
results on the `stress_collision/` scenarios if collisions actually merge -
`0050`/`0056`/pre-fix `0063` all have the folder-merge bug, so root-collision timings
against them are measuring "how fast does it return one wrong (merged) folder", not
a fair comparison. That's expected and part of the point.

**Restoring the current function afterward:**

```bash
psql "$DB_URL" -f ../../migrations/tenant/0063-fix-search-name-relative-to-prefix.sql
```

`npm run migration:run` will *not* do this for you if the tracked migration hash
already matches the file on disk - it only updates the recorded hash, it doesn't
re-run the migration. Re-applying the file directly is the reliable way to make sure
the live function matches what's on disk again.

## EXPLAIN ANALYZE

```bash
./run.sh --rows 1000     --phase schema,seed,explain --label small
./run.sh --rows 10000000 --phase schema,seed,explain --label large
```

`sql/20_explain.sql` runs two kinds of EXPLAIN:
1. `storage.search(...)` as a black box (`EXPLAIN ANALYZE SELECT * FROM
   storage.search(...)`). Since it's a PL/pgSQL function, the planner shows it as a
   single `Function Scan` node with the real total time/rows - useful for "does this
   scale with N" but not for seeing which internal step is slow.
2. The individual queries the function actually runs internally (the peek query,
   the batch fetch, the non-name-sort ILIKE+GROUP BY query, the collision existence
   check), run standalone with realistic bind values pulled from the seeded data, so
   you can see the actual index/scan choice for each hot-path piece.

## Cleanup

```bash
./run.sh --phase cleanup                      # drops the seeded bucket/objects
./run.sh --phase cleanup --drop-perf-schema   # also drops perf.* bookkeeping entirely
```

## All options

```
--rows N              base row count for the clean dataset (default 1000)
--collision-rate R    fraction of folders given a case-flipped twin (default 0.05)
--bucket-id ID        test bucket id (default perf-stress-test)
--batch-size N        rows per seed INSERT/COMMIT batch (default 200000)
--bench-limit N       LIMIT used for every benchmarked search() call (default 100)
--sort-columns EXPR   SQL array literal of sort columns to benchmark, e.g.
                       "ARRAY['name']" to skip the (slow, unrelated) non-name-sort
                       path and iterate much faster at large scale
                       (default: "ARRAY['name', 'created_at']")
--function PATH       apply this .sql file before bench/explain (see above)
--label NAME          label recorded with results (default: --function's basename, or "current")
--phase LIST          comma-separated: schema,seed,bench,explain,export,cleanup
                       (default: schema,seed,bench,export)
--db-url URL          postgres connection string
                       (default: $PERF_DATABASE_URL or postgresql://postgres:postgres@localhost:5432/postgres)
--run-id ID           reuse a specific run id (default: generated timestamp)
--drop-perf-schema    when the cleanup phase runs, also drop the perf schema
```
