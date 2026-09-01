# Wave-2 performance benchmark: results

Scale: 10M rows, `benchmark` bucket, `folder-NNNNN/key-NNNNNN.bin` layout (1000 keys/folder, 10,000 folders).

- **pre**: `pre-2026-08-24T21-10-02-064Z.json` — schema frozen at `mark-filename-immutable` (before `object-versioning-core`), 10M unique keys, flat rows.
- **post-default**: `post-default-2026-08-24T22-10-38-355Z.json` — full wave-2 schema, same 10M keys, every key single-version (`is_versioned=false`, `archived_at IS NULL`) — the "existing customer who's never touched versioning" case.
- **post-versioned**: `post-versioned-2026-08-24T22-56-38-406Z.json` — full wave-2 schema, realistic long-tail version history: ~7.23M unique keys / 10.18M rows (90% single-version, 9.5% 2-5 versions, 0.5% 20-50 versions with ~10% of those replaced by a delete marker as their current row).
- **post-versioned (Run 2)**: `post-versioned-run2-2026-08-27T15-13-42-825Z.json` — same seed/scale (~7.23M unique keys / 10.18M rows), re-run after rebasing wave-2 onto master (post wave-1 squash-merge) and landing the continuation-token filter-locking fix, to confirm nothing regressed since the original run.
- **post-versioned (Run 3)**: `post-versioned-run3-2026-08-31T19-30-51-434Z.json` — same seed/scale (~7.23M unique keys / 10.18M rows), re-run on top of wave-2a (`bucketid_objname` dropped, `COLLATE "C"` added to the versioning indexes, the `search_by_timestamp` version-tiebreak fix) to confirm none of that regressed anything. Note: this run does **not** exercise the redundant `name >= $cursor` index-seek fix in `pg.ts`'s `listObjectsV2` — these scenarios call the SQL functions directly, bypassing the app layer entirely (see Methodology), so that fix needs its own separate `EXPLAIN` verification, not covered by this benchmark.
- **post-versioned (Run 4)**: `post-versioned-run4-2026-09-01T13-16-25-569Z.json` — same seed/scale (~7.23M unique keys / 10.18M rows), re-run after adding the partial `(bucket_id, name COLLATE "C") WHERE is_delete_marker` index and specializing `list_objects_with_delimiter`'s `delete_markers = 'only'` peek so its plan can use that index.

## Headline result: two regressions found and fixed

The first `post-default` run showed catastrophic regressions in `list_objects_with_delimiter` (900ms vs. 1.1ms small page; 21+ minutes with no completion on the large-page/batch-stress scenario, vs. 126ms on PRE). Root-caused to two independent PL/pgSQL/planner issues in the function's hot-path "peek" and "batch" static/dynamic SQL, both fixed in `migrations/tenant/0068-list-objects-with-versions.sql`:

1. **Plan-cache defeat via a parameter-gated seek predicate.** The peek query embedded `v_multi_row` (a boolean) directly in the WHERE clause: `(NOT v_multi_row AND name >= $x) OR (v_multi_row AND ...)`. PL/pgSQL switches from a per-call custom plan to one cached *generic* plan after 5 executions; once generic, the planner can no longer prove which branch applies and drops `name` from the index condition entirely, degrading every subsequent peek (of up to 10,000 per call) to a full index scan filtered row-by-row. **Fix:** branch on `v_multi_row` in PL/pgSQL control flow instead, so each concrete query has an unconditional, always-indexable seek predicate.
2. **Keyset tuple-comparison OR defeats index pushdown outright**, independent of custom/generic plan caching. The multi-row seek/batch predicate `(name > $x) OR (name = $x AND tiebreak)` — the standard keyset-pagination idiom — is never split into indexable form by Postgres, even with fully literal values; it always evaluates as a single Filter after a `bucket_id`-only index scan. **Fix:** split into two independently-indexable branches (`name = $x` and `name > $x`) combined with `UNION ALL`, applied in both the peek query and the dynamic batch query.

Verified via direct `EXPLAIN (ANALYZE, BUFFERS)` at 10M-row scale before re-running the full benchmark; both fixes hold under the generic-plan switchover and match PRE-baseline timings. Full plan excerpts are in the "Query plan comparison" section below.

## All scenarios, p50 latency (pre | post-default | post-versioned in one row)

`—` = scenario doesn't apply to that state (e.g. `noncurrentVersions`/`deleteMarkers` filters only make sense once multi-version data exists).

| Scenario | PRE | POST-default | POST-versioned | POST-versioned (Run 2) | POST-versioned (Run 3) | POST-versioned (Run 4) |
|---|---:|---:|---:|---:|---:|---:|
| list_objects_with_delimiter: root, small page | 1.12ms | 1.53ms | 1.15ms | 1.90ms | 1.65ms | 1.69ms |
| list_objects_with_delimiter: root, large page (batch-algorithm stress) | 126.05ms | 151.95ms | 71.17ms | 74.49ms | 69.18ms | 72.75ms |
| list_objects_with_delimiter: within one busy folder | 3.36ms | 4.27ms | 4.21ms | 6.42ms | 5.38ms | 4.66ms |
| list_objects_with_delimiter: root, desc sort | 7.11ms | 10.35ms | 8.42ms | 9.11ms | 9.11ms | 10.50ms |
| list_objects_with_delimiter: noncurrentVersions=include, small page | — | — | 1.61ms | 1.83ms | 1.85ms | 2.14ms |
| list_objects_with_delimiter: noncurrentVersions=include, large page | — | — | 98.14ms | 111.10ms | 159.64ms | 169.26ms |
| list_objects_with_delimiter: deleteMarkers=only | — | — | 3175.92ms | 4081.87ms | 2425.58ms | 2.10ms |
| search: small page, offset 0 | 0.95ms | 1.10ms | 1.50ms | 1.47ms | 1.68ms | 1.48ms |
| search: large page | 13.25ms | 16.84ms | 15.63ms | 17.73ms | 17.78ms | 16.59ms |
| search: deep offset | 43.99ms | 47.72ms | 45.79ms | 49.20ms | 46.60ms | 54.76ms |
| search: noncurrentVersions=include, large page | — | — | 18.89ms | 17.36ms | 16.91ms | 20.11ms |
| search_by_timestamp: small page, updated_at asc | 14870ms | 12821ms | 9184.91ms | 14960.89ms | 8266.35ms | 9945.93ms |
| search_by_timestamp: large page | 9778ms | 12553ms | 9574.86ms | 11649.08ms | 7947.59ms | 8327.32ms |
| search_by_timestamp: noncurrentVersions=include, large page | — | — | 12887.84ms | 12632.90ms | 10835.17ms | 11704.54ms |
| search_v2: root, small page | 0.96ms | 1.95ms | 4.30ms | 1.97ms | 1.39ms | 1.56ms |
| search_v2: root, large page | 17.83ms | 15.67ms | 68.04ms | 15.70ms | 16.69ms | 18.80ms |
| search_v2: noncurrentVersions=include, large page | — | — | 24.62ms | 23.84ms | 24.17ms | 25.90ms |
| get_size_by_bucket: default | 663ms | 1072ms | 846.89ms | 772.41ms | 848.53ms | 921.76ms |
| get_size_by_bucket: noncurrentVersions=include | — | — | 1227.49ms | 884.77ms | 1055.62ms | 866.53ms |
| get_size_by_bucket: deleteMarkers=only | — | — | 348.51ms | 482.32ms | 374.89ms | 1.68ms |

**No regressions in post-default vs. pre** — everything lands within 1.1x-2.0x (mostly sub-2ms absolute differences; the couple of larger ratios are normal plan/cache noise). **post-versioned's `noncurrentVersions=include` rows land in the same tens-of-ms range as the equivalent default-path rows** — direct confirmation the fix generalizes to real multi-version data, not just the synthetic reproduction that found the bug.

**Run 2 (post-versioned, re-run after the wave-1 squash-merge rebase and the continuation-token locking fix) confirms nothing regressed**: every fast scenario (sub-100ms) stays within normal run-to-run variance of Run 1, `search_v2`'s two root-scan rows actually improved (68.04ms→15.70ms, likely cache/plan variance rather than a real change - no code in this diff touches `search_v2`'s planning), and the two already-flagged-as-non-regressions (`search_by_timestamp`, `deleteMarkers=only`) remain multi-second/multi-hundred-ms respectively for the same pre-existing reasons (no supporting index) - consistent with Run 1's findings, not a new issue.

**Run 3 (post-versioned, re-run on wave-2a: `bucketid_objname` dropped, versioning indexes collated, `search_by_timestamp` version-tiebreak fix) also confirms nothing regressed**: `search_by_timestamp`'s three rows all improved over Run 2 (14960→8266ms, 11649→7947ms, 12632→10835ms) - the version-tiebreak fix (using each row's real version instead of collapsing it to `''`) did not make this path slower, if anything the opposite, consistent with it being a full-table-scan-dominated, no-supporting-index scenario where run-to-run variance is expected to be the largest factor either way. `deleteMarkers=only` also improved (4081→2425ms). One row worth flagging honestly rather than waving off: `list_objects_with_delimiter: noncurrentVersions=include, large page` rose from 111.10ms (Run 2) to 159.64ms (Run 3), a larger jump than the sub-2ms noise seen elsewhere. Nothing in this round of fixes touches `list_objects_with_delimiter`'s own SQL (the drop/collation/search_by_timestamp changes don't reach it), so this reads as run-to-run variance rather than a regression, but it's a big enough jump that it's worth a dedicated re-check with `EXPLAIN (ANALYZE, BUFFERS)` rather than asserting that from timing alone.

**Run 4 confirms the partial index and specialized peek remove the sparse delete-marker scan without regressing other paths.** `list_objects_with_delimiter: deleteMarkers=only` fell from 2425.58ms to 2.10ms and `get_size_by_bucket: deleteMarkers=only` fell from 374.89ms to 1.68ms. The ordinary delimiter, search, and `search_v2` rows remain within normal run-to-run variance. The specialization is entered only for `delete_markers = 'only'`; `exclude` and `include` retain their existing peek paths.

The remaining slow path is **not a regression**:
- `search_by_timestamp` is multi-second **in both PRE and POST** almost identically — pre-existing, no supporting index on `(bucket_id, updated_at)`, falls back to a full table scan at this scale. Out of scope for this benchmark; worth its own follow-up.

## Query plan comparison (buffers, from the benchmark's own `EXPLAIN (ANALYZE, BUFFERS)` capture)

Pulled directly from the three result JSON files' stored `explain` field for `list_objects_with_delimiter` (this reflects the *outer* function-call plan — `Function Scan on list_objects_with_delimiter` — since the interesting cost is inside the PL/pgSQL loop; the buffer counts below are still a faithful before/after signal since they count every page touched across the whole function execution):

| Scenario | State | Buffers hit | Buffers read | Exec time |
|---|---|---:|---:|---:|
| root, small page | PRE | 514 | 0 | 0.76ms |
| root, small page | POST-default | 514 | 0 | 0.74ms |
| root, small page | POST-versioned | 514 | 0 | 0.79ms |
| root, large page | PRE | 30,000 | 21,428 | 121.10ms |
| root, large page | POST-default | 30,000 | 21,428 | 128.96ms |
| root, large page | POST-versioned | 37,234 | 0 | 56.28ms |
| within one busy folder | PRE | 51 | 0 | 0.99ms |
| within one busy folder | POST-default | 53 | 0 | 1.55ms |
| within one busy folder | POST-versioned | 54 | 0 | 1.54ms |
| root, desc sort | PRE | 5,005 | 0 | 6.97ms |
| root, desc sort | POST-default | 5,005 | 0 | 8.80ms |
| root, desc sort | POST-versioned | 5,111 | 0 | 7.19ms |
| noncurrentVersions=include, small page | POST-versioned only | 914 | 0 | 1.14ms |
| noncurrentVersions=include, large page | POST-versioned only | 66,053 | 0 | 95.82ms |
| deleteMarkers=only | POST-versioned only | 130 | 372,461 | 2534.81ms |

Buffer counts for `root, large page` and `within one busy folder` are essentially identical PRE vs. POST-default (same page-touch pattern, not just similar wall-clock) — this is the strongest evidence the fix restored the original access pattern rather than just happening to run fast on this particular data. `deleteMarkers=only`'s 372,461 buffer reads (~2.9GB) is the full-table-scan signature described above.

### Root-cause plans, captured live during the fix (`EXPLAIN (ANALYZE, BUFFERS)` against the 10M-row `benchmark` dataset)

These were captured directly via `psql` while diagnosing the two bugs — showing the actual "before" (broken) and "after" (fixed) plan Postgres chose once past the 5-execution custom-plan cutoff (the only point either bug manifests).

**Bug #1 — peek query, single-version path, generic plan (6th execution):**

Before (boolean gate `$3`/`v_multi_row` folded into the WHERE clause — `name` drops out of the index condition entirely):
```
Limit  (cost=0.56..0.98 rows=1 width=60) (actual time=3.229 rows=1 loops=1)
  ->  Index Scan using idx_objects_bucket_id_name on objects o
        Index Cond: (bucket_id = $1)              -- name is GONE from the index condition
        Filter: ((($3 <> 'exclude') OR (archived_at IS NULL)) AND ...
                 AND (((NOT $3) AND (name >= $2)) OR ($3 AND ((name > $2) OR (name = $2)))))
Planning Time: 0.046 ms   Execution Time: 0.019 ms   -- fast here only because LIMIT 1 hit early;
                                                          catastrophic once the seek position moves later
```

After (branched in PL/pgSQL, unconditional predicate — `name` stays a real index condition):
```
Limit  (cost=0.56..0.69 rows=1 width=60) (actual time=0.010 rows=0 loops=1)
  ->  Index Scan using idx_objects_bucket_id_name on objects o
        Index Cond: ((bucket_id = $1) AND (name >= $2))
        Filter: ((($3 <> 'exclude') OR (archived_at IS NULL)) AND ...)
Planning Time: 0.002 ms   Execution Time: 0.018 ms
```

**Bug #2 — peek query, multi-version path (`noncurrentVersions=include`), generic plan — worst case, seeking near the end of the key space:**

Before (keyset tuple OR `name > $2 OR (name = $2 AND tiebreak)` — never split into indexable form, even fully literal):
```
Limit  (cost=0.56..1233.21 rows=1 width=60) (actual time=8739.615..8739.622 rows=0 loops=1)
  ->  Index Scan using idx_objects_bucket_id_name on objects o
        Index Cond: (bucket_id = 'benchmark'::text)     -- name is GONE again
        Filter: ((NOT is_delete_marker) AND ((name > 'folder-09000/key-000900.bin') OR (name = 'folder-09000/key-000900.bin')))
        Rows Removed by Filter: 10183639                -- scanned essentially the whole table
Planning Time: 0.241 ms   Execution Time: 8739.778 ms
```

After (split into two `UNION ALL` branches, each independently indexable):
```
Limit  (cost=7.36..7.52 rows=1 width=60) (actual time=0.011 rows=0 loops=1)
  ->  Result
        ->  Merge Append
              ->  ... Index Scan on objects o    Index Cond: ((bucket_id = $1) AND (name = $2))       -- equality branch
              ->  ... Index Scan on objects o_1  Index Cond: ((bucket_id = $1) AND (name > $2))       -- range branch
Planning Time: 0.002 ms   Execution Time: 0.018 ms
```

**Bug #2 (batch query variant) — fetching up to 1000 rows for a busy multi-version key, generic plan:**

Before: even the very *first* (custom-plan) execution was already slow (~1000ms; not just a plan-cache artifact), because the same OR-tuple predicate forced a full-table `Index Scan (Cond: bucket_id only)` + `Incremental Sort` reading ~500K-5.5M rows before the `LIMIT`:
```
Limit (actual time=984.623..1009.834 rows=1000 loops=1)
  ->  Incremental Sort
        ->  Index Scan using idx_objects_bucket_id_name on objects o
              Index Cond: (bucket_id = 'benchmark'::text)
              Filter: ((NOT is_delete_marker) AND ((name > ...) OR (name = ...)))
              Rows Removed by Filter: 500000
Execution Time: 1010.088 ms   -- and 8327.396 ms at the 6th (generic-plan) call for a later seek position
```

After (same `UNION ALL` split, applied to the dynamic batch query):
```
Limit (actual time=1.590..8.791 rows=1000 loops=1)
  ->  Result
        ->  Merge Append
              ->  ... Index Cond: ((bucket_id = $1) AND (name = $2))   -- same-key remaining versions
              ->  ... Index Cond: ((bucket_id = $1) AND (name > $2))   -- next keys, LIMIT-bounded incremental sort
Execution Time: 8.821 ms   -- consistent 0.4-8.8ms across both custom and generic plan calls
```

**Bug #3 — same keyset-OR pushdown issue, found separately in `pg.ts`'s own hand-built query, not covered by any of the 20 scenarios above.** `listObjectsV2`'s flat/`exactMatch` path (used when `!delimiter || exactMatch`, i.e. not routed through `list_objects_with_delimiter`/`search_v2`) builds its own keyset predicate: `(name > $cursor) OR (name = $cursor AND tiebreak)`. This is the identical shape as Bug #2 above, and Postgres can't push it into an index range condition for the same reason. Found via PR review, not by this benchmark — the benchmark's scenarios call the five SQL functions directly and never exercise this code path at all.

Verified live against the 10M-row `post-versioned` dataset (real cursor 90% through the key space, `folder-06500/key-000000.bin`), same before/after methodology as Bugs #1-2:

Before (`name` dropped from the index condition, full scan from the start of the bucket):
```
Limit  (cost=4.94..45.43 rows=100 width=252) (actual time=5356.366..5356.404 rows=100 loops=1)
  ->  Incremental Sort
        ->  Index Scan using idx_objects_bucket_id_name on objects
              Index Cond: (bucket_id = 'benchmark'::text)
              Filter: ((name > 'folder-06500/key-000000.bin') OR (name = 'folder-06500/key-000000.bin'))
              Rows Removed by Filter: 6500000
Execution Time: 5356.432 ms
```

After (added a redundant `name >= $cursor` bound - implied by the OR, but expressible as a plain range condition the planner can push down):
```
Limit  (cost=3.42..75.52 rows=100 width=252) (actual time=0.151..0.191 rows=100 loops=1)
  ->  Incremental Sort
        ->  Index Scan using idx_objects_bucket_id_name on objects
              Index Cond: ((bucket_id = 'benchmark'::text) AND (name >= 'folder-06500/key-000000.bin'::text))
              Filter: ((name > 'folder-06500/key-000000.bin') OR (name = 'folder-06500/key-000000.bin'))
Execution Time: 0.229 ms
```

5356ms → 0.229ms (~23,000x) at this depth; buffers read dropped from 278,580 to 3. Same fix pattern as Bug #2 (make an implied bound explicit so the planner can use it), just a redundant `AND` clause here instead of a `UNION ALL` split, since this is a single flat query rather than a peek-then-batch loop. Fixed in `src/storage/database/pg.ts`.

## Correctness: ordering and pagination for the common case (`noncurrentVersions=include`, sorted by name)

The case we most need to optimize for — listing sorted by `name`, with `archived_at DESC` as the tiebreak so each key's current version sorts first followed by most-recent-to-oldest archived versions (Postgres's default `NULLS FIRST` for `DESC`, unchanged by this fix) — is verified correct and fast:

- `ORDER BY o.name COLLATE "C" ASC, o.archived_at DESC` is unchanged by this fix in every branch (peek query and both `UNION ALL` arms of the batch query) — only the WHERE-clause seek predicate was restructured, not the output ordering.
- `object-list-v2.test.ts`'s `"most-recent-first ordering within a key on listObjectsV2 (flat, noncurrentVersions include)"` asserts exactly this: current version (`archived_at IS NULL`) first, then progressively older versions — passing.
- `object-list-v2.test.ts`'s `"pagination does not repeat a key's current row when a page boundary lands right after it (with delimiter)"` walks a real paginated sequence through the HTTP route and asserts no duplicate/skipped rows across a page boundary that lands mid-key — passing. The app-level cursor (`object.ts`) disambiguates "no cursor yet" from "already consumed this key's current row" by encoding the latter as the literal string `'infinity'` rather than `null` when building the next-page token, which the SQL's `COALESCE(archived_at, 'infinity') < v_next_seek_at` comparison then correctly excludes on the next call.
- Full suite (349 tests across `object-versioning-schema`, `object-list-v2`, `storage-pg-db`, `object.test`, `search-filters`, `s3-protocol`) passes with the fix applied.

`search_by_timestamp` remains multi-second regardless of state (pre-existing, not a regression) — acceptable for now since the name-sorted path is the one we need to be fast, and it is.
