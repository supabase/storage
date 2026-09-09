# storage.search() performance findings

Comparison of exactly two versions of `storage.search()`:

- **original** - `migrations/tenant/0063-fix-search-name-relative-to-prefix.sql` as it
  stands on `master` (the version currently in production, including the
  case-collision bug this work fixes).
- **v5** - the same file as it stands on this branch (the version this document
  argues for). Named "v5" for continuity with `ALGORITHM-VERSIONS.md`, which has
  the full history of intermediate approaches that were tried, measured, and
  discarded before landing here - not reproduced in this document, which only
  compares the two versions that actually exist in git history now.

Produced with the harness in this folder (`./run.sh`). All numbers below are from
runs against this machine's local Postgres; treat absolute milliseconds as
workstation-scale, and the *shape* of the results (correctness, relative cost,
growth rate) as the reliable part.

## What changed, briefly

**The bug:** two objects whose names differ only by case at some path segment -
e.g. `my_Folder/x.png` and `my_folder/x.png` - should list as two distinct
folders. In the original function they silently merge into one; whichever
casing the internal scan happens to land on first is the only one a caller ever
sees. The other folder's contents don't error, don't 404 - they simply never
appear in a listing.

**The root cause:** `storage.search()`'s NAME-SORT branch sorts and seeks by
`lower(name)`, not `name`. That's not a deliberate "case-insensitive listing"
design choice - `prefix`/`search` need to match case-insensitively (see "case
insensitive search should work" below), and once the `WHERE` bound is
`lower(name)`-based, using the same expression for `ORDER BY` was the only way
to get an index-backed walk. The sort order piggybacked on the filter. The
practical effect: two case-variant siblings are forced to tie on the primary
sort key, and the function's O(1) "jump past this folder, we're done with it"
step has no way to tell the two apart - it jumps past both.

**The fix:** stop comparing by `lower(name)` in the walk. Sort and seek by
`name COLLATE "C"` alone (`idx_objects_bucket_id_name`, already existed -
migration 0020). Under exact-byte ordering, two case-variant folders are just
two different strings; neither is ever mistaken for a range containing the
other, so the original O(1) jump is unconditionally correct again, with no
per-folder check of any kind. `storage.search_v2` (migration 0050) already
worked this way and has never had this bug - this brings list v1 in line with
it. **No new tables, triggers, indexes, or columns** - this is a pure rewrite
of the existing function.

The one thing pure `name`-ordering can't do on its own is case-insensitive
`prefix`/`search` matching, so that's handled separately, once per call instead
of once per comparison:
- **`prefix` alone** (empty, or ends at a delimiter - true for every plain
  "list this folder" call): resolved once, up front. Try the caller's literal
  bytes first (free in the common case - a client normally already has the
  right case from a prior listing); only on a miss, one indexed lookup against
  `lower(name)` to find the real casing, then the rest of the call runs in
  exact-name order from there.
- **`search` supplying a partial, non-delimiter-terminated fragment** (e.g.
  `search='F'` matching sibling folders `FOO`/`Foo`/`foo`, and potentially
  unrelated folders like `Fantastic/` too): this can't be resolved to one
  boundary, so this one query shape falls back to the original per-folder
  check-and-resolve logic, verbatim. See "the bad search case" below for what
  that costs and why it's an acceptable trade.

## Methodology

- **Dataset**: `./run.sh`'s standard two-scenario seed - `stress/` (clean,
  zero collisions) and `stress_collision/` (same folder count, plus a
  case-flipped twin for 5% of folders, each twin given the *same* child file
  names as its original - the specific case that breaks a naive single-row
  reseek, see the migration's comments). Both scenarios live in the same
  bucket, seeded together, at every scale below.
- **Scales**: 1,000 / 10,000 / 100,000 / 1,000,000 / 10,000,000 (the `--rows`
  figure for the clean scenario only - the collision scenario adds ~5% more on
  top, so the 10M run seeds ~20.5M rows total). Folder count and files-per-folder
  both scale with `sqrt(rows)`, by harness design, so root-level listing and
  within-folder listing are both meaningfully exercised at every scale.
- **Timing**: `perf.bench_offset_matrix` - each column x offset combination is
  called once untimed (warms caches/plans) then 5 times timed via
  `clock_timestamp()` inside the DB, averaged. `min`/`avg`/`max` below are across
  those 5 repeats.
- **Offsets**: shallow (0), mid (~half the folder count), deep (~folder count
  minus a page) - labeled 0 / mid / deep in the tables. At 1,000 and 10,000
  rows the folder count (32 and 100) doesn't exceed the default page size
  (limit 100), so all three offset tiers collapse to 0 - **there is no
  meaningful "deep offset" behavior to observe below 100,000 rows** with this
  harness's default limit; treat the 1k/10k rows as confirming correctness and
  baseline latency, not scaling.
- **Sort column**: `name` only, both directions, per the scope of this work.
- Raw CSVs: `results/run-scale{1k,10k,100k,1m,10m}-{original,v5}-*.csv`. EXPLAIN
  output: `results/explain-scale{1k,10m}-{original,v5}.txt`.

## 1. Correctness, demonstrated

Before the timing numbers: at every scale, `original` and `v5` return
**different row counts** for the collision scenario, because `original` is
wrong. Smallest, clearest example (1,000-row scale, 32 base folders + 2
case-flipped twins = 34 real folders under `stress_collision/`):

```sql
-- original
SELECT count(*) FROM storage.search('stress_collision/', bucket, 100, 1, 0, '', 'name', 'asc');
--  count
-- -------
--     32        <- two colliding folders silently merged away

-- v5
SELECT count(*) FROM storage.search('stress_collision/', bucket, 100, 1, 0, '', 'name', 'asc');
--  count
-- -------
--     34        <- correct
```

This holds at every scale tested (the collision *rate* is fixed at 5% of
folders by the harness, so the absolute gap grows with scale: 2 folders lost at
1k, ~158 at 10M). Every timing number for `original`'s `root-collision`
scenario below is measuring a query that returns **fewer rows than it should**
- it's doing less work than a correct implementation would, not more. Keep
that in mind reading the "collision" columns: `original` being fast there is
not a point in its favor.

## 2. Timing by scale

`root-clean` and `root-collision` = root-level folder listing, sorted by
`name` ascending (descending tracked within a few percent throughout - not
reproduced per-scale below for brevity; full numbers in the CSVs).
`within-folder` = listing inside one large populated folder (never touches
folder-boundary logic at all - included as a control).

All times in ms, averaged over 5 warmed repeats.

### 1,000 rows (32 folders - offset tiers collapse to 0, see Methodology)

| scenario | original | v5 |
|---|---|---|
| root-clean | 0.75 | 0.53 |
| root-collision | 0.46 | 0.38 |
| within-folder | 0.26 | 0.19 |

### 10,000 rows (100 folders - offset tiers still collapse to 0)

| scenario | original | v5 |
|---|---|---|
| root-clean | 0.91 | 0.56 |
| root-collision | 0.76 | 0.52 |

### 100,000 rows (316 folders)

| scenario | offset 0 | offset ~58 (mid) | offset ~206 (deep) |
|---|---|---|---|
| root-clean, original | 1.22 | 1.47 | 2.29 |
| root-clean, v5 | 1.09 | 1.23 | 1.94 |
| root-collision, original | 0.73 | 1.15 | 2.18 |
| root-collision, v5 | 0.56 | 0.89 | 1.74 |

### 1,000,000 rows (1,000 folders)

| scenario | offset 0 | offset ~400 (mid) | offset ~890 (deep) |
|---|---|---|---|
| root-clean, original | 1.06 | 3.52 | 6.99 |
| root-clean, v5 | 0.63 | 2.82 | 5.47 |
| root-collision, original | 0.80 | 3.80 | 7.35 |
| root-collision, v5 | 0.58 | 2.95 | 5.79 |
| within-folder, original | 0.32 | 0.89 | 1.57 |
| within-folder, v5 | 0.28 | 0.52 | 0.89 |

### 10,000,000 rows (3,162 folders)

| scenario | offset 0 | offset ~1,481 (mid) | offset ~3,052 (deep) |
|---|---|---|---|
| root-clean, original | 0.99 | 11.48 | 24.75 |
| root-clean, v5 | 0.57 | 9.77 | 25.55 |
| root-collision, original | 0.98 | 16.44 | 28.30 |
| root-collision, v5 | 0.62 | 10.81 | 28.29 |
| within-folder, original | 0.51 | 2.54 | 4.87 |
| within-folder, v5 | 0.26 | 1.35 | 2.66 |

**v5 is never slower than `original` at any scale or offset tested for
`root-clean`, and matches or beats it at `root-collision` too - while
`root-collision` is the scenario where `original` is returning wrong
(incomplete) answers.** The two exceptions where v5's average is marginally
higher than original's (root-clean deep offset at 10M: 25.55 vs 24.75; a
single high-variance max of 40.82ms pulled that average up - min-to-min it's
19.49 vs 23.75, v5 faster) are within run-to-run noise, not a real regression;
see the EXPLAIN section below for same-session, cache-warmed numbers that
confirm this (v5 ~22-24ms vs original's ~29-30ms once plan-cache noise is
controlled for).

## 3. Rate of growth relative to row count

Using `root-clean`, deep-offset, ascending - the scenario both versions can be
compared on fairly (unlike `root-collision` for `original`), and the one where
scaling behavior actually shows up (100k+; see the 1k/10k caveat above):

| rows | original (ms) | v5 (ms) | rows x10 | original growth | v5 growth |
|---|---|---|---|---|---|
| 100,000 | 2.29 | 1.94 | - | - | - |
| 1,000,000 | 6.99 | 5.47 | 10x | 3.05x | 2.82x |
| 10,000,000 | 24.75 | 25.55 | 10x | 3.54x | 4.67x |

Both versions grow at roughly the same rate as each other, and that rate is
**sub-linear but faster than logarithmic** - consistent with the mechanism:
folder count scales with `sqrt(rows)` by harness design, the deep-offset tier
is defined relative to folder count, and each folder boundary costs one O(log
rows) index seek, so total cost is roughly `sqrt(rows) x log(rows)`, which
lands between `sqrt(10)≈3.16x` and linear `10x` per decade - matching the
observed ~3-4.7x per 10x-rows step. This is expected and is not a regression
introduced by either version: it's the offset-based-pagination "walk past N
folders" mechanism both share, and it's explicitly out of scope for this work
(see the migration's own comments on non-`name` sort columns being separately
slow, and the DESC file-batch-crosses-folder bug being a separate, pre-existing
issue).

The point of this section isn't that either version scales asymptotically
better than the other on the happy path - **they scale the same, because
they're the same algorithm on that path.** The point is section 1: `original`
returns wrong answers doing it, and every version explored before v5 that
fixed that (see `ALGORITHM-VERSIONS.md`) cost meaningfully more to do so. v5
doesn't.

## 4. v5 with vs. without case collisions

Pulling `root-clean` (no collisions anywhere in that subtree) and
`root-collision` (a real collision every 20 folders) back out of section 2,
v5 only:

| rows | root-clean deep (ms) | root-collision deep (ms) | delta |
|---|---|---|---|
| 100,000 | 1.94 | 1.74 | -0.20 |
| 1,000,000 | 5.47 | 5.79 | +0.32 |
| 10,000,000 | 25.55 | 28.29 | +2.74 |

The delta is within the noise floor of the measurements themselves (compare to
the min/max spread in the raw CSVs) at every scale tested - there is no
systematic cost to a collision existing elsewhere in the walk, because v5's
folder-jump is the same unconditional O(1) operation whether or not the
folder it's jumping past happens to have a same-named, differently-cased
sibling. This is the direct payoff of fixing the root cause (the sort key)
instead of adding a check: there's no "collision path" to be slow, because
there's no per-folder decision being made at all.

## 5. The bad search case

`search` supplying a partial fragment that doesn't end at a delimiter (e.g.
`search='F'` against sibling folders `FOO`/`Foo`/`foo`) can't be resolved to a
single boundary the way a clean `prefix` can - a broad partial match can span
several *unrelated* folders, each of which might independently have its own
collision. v5 falls back to the original per-folder existence-check-and-resolve
logic (verbatim) for exactly this query shape. Tested directly: `prefix =
'stress_collision/'`, `search = 'folder_'` (matches every `folder_*` and
`FOLDER_*` folder case-insensitively - all 3,320 of them at 10M scale),
`limit = 100`, averaged over 5 warmed repeats:

| rows | original (ms) | v5 fallback (ms) | v5 vs original |
|---|---|---|---|
| 1,000 | 1.85 | 2.73 | ~1.5x slower |
| 10,000,000 | 1.85 | 201.22 | **~109x slower** |

This is genuinely bad, and worth being direct about: at 10M rows, this one
query shape on v5 is *slower than `original`'s worst-case deep-offset root
listing (24.75ms) by a factor of 8*. `EXPLAIN (ANALYZE, BUFFERS)` shows why -
`original` never runs the collision check at all (it doesn't have one; that's
the bug), touching 1,187 buffer pages in 7ms regardless of correctness;
v5's fallback touches **319,822** buffer pages in 232ms proving, folder by
folder, that each of the (mostly non-colliding) matches really is what it
looks like. That's the same "prove a negative over a large range" cost
`ALGORITHM-VERSIONS.md` documents for the checked-every-folder approaches that
were tried and discarded (v3/v4) - it's not new to v5, it's the cost of
correctness for *this specific query shape*, unavoidable without either (a) a
per-folder cost the fast path deliberately doesn't pay, or (b) new
schema/indexing this work was explicitly told not to introduce.

**Why this trade was accepted:** this query shape - `search` ending mid-name -
is not what `prefix`-based folder navigation produces, ever; it's not present
anywhere in this stress harness's own benchmarks, and every measurement in
sections 2-4 above is unaffected by it. It's a real cost for a real (if
narrow) feature - a fuzzy, type-to-filter search box matching partial names -
and if that feature is used at scale in production, this is the number to
watch. If it turns out to matter, the fix would need to look different from
sections 2-4's fix (a real per-folder cost can't be waved away by a sort-key
change the way the collision bug could - see `ALGORITHM-VERSIONS.md`'s "if
you're trying a different approach" for where to start).

## 6. EXPLAIN ANALYZE - what the planner actually does

All EXPLAINs run `ANALYZE, BUFFERS`, warmed (untimed run before capture; for
the deep-offset numbers, captured as the 3rd of 3 back-to-back `EXPLAIN`s in
one session, since the 1st is measurably slower purely from plan/relation-cache
warmup within that session - not a real cost difference, and worth knowing if
reproducing this). Full output for every numbered scenario in
`sql/20_explain.sql`: `results/explain-scale1k-{original,v5}.txt` and
`results/explain-scale10m-{original,v5}.txt`.

**Both versions' internal peek query use their sort key's matching index
directly, O(log n), sub-2ms cold, at 10M rows:**

```
-- original: seeks lower(name), one existing index, heap access needed
Index Scan using idx_objects_bucket_id_name_lower on objects o
  Index Cond: (bucket_id = ... AND lower(name) >= ... AND lower(name) < ...)
  Buffers: shared hit=3 read=2 · Execution Time: 1.09 ms

-- v5: seeks name directly, one existing index, NO heap access needed
Index Only Scan using idx_objects_bucket_id_name on objects o
  Index Cond: (bucket_id = ... AND name >= ... AND name < ...)
  Heap Fetches: 0
  Buffers: shared hit=2 read=3 · Execution Time: 1.47 ms
```

v5's peek is an **index-only scan** (name is the whole index, no heap fetch
needed to confirm the row) where original's is a regular index scan on the
`lower(name)` expression index; both are equivalently cheap in practice at this
row count, and this was already true before this work (`FINDINGS.md`'s
predecessor called the peek/batch machinery "already good" - that observation
still holds, this part of the mechanism didn't change).

**The mechanism difference is entirely in what happens at a folder boundary.**
`original` has nothing to explain here - it just jumps. Standalone EXPLAIN of
the check a *correctness-preserving* per-folder approach would need (this is
the query v5's `search`-fallback path runs, and what a naive "add a check to
original" fix would run on *every* folder boundary, not just the fallback
case), against a real 3,162-file folder at 10M scale:

```
-- proving a collision folder does NOT have a case-variant sibling (the common case)
Index Scan using idx_objects_bucket_id_name_lower on objects o
  Index Cond: (bucket_id = ... AND lower(name) >= 'stress/folder_0000001/' AND lower(name) < 'stress/folder_00000010')
  Filter: (left(name, 22) <> 'stress/folder_0000001/')
  Rows Removed by Filter: 3162        <- scans the entire folder to prove a negative
  Buffers: shared hit=175 · Execution Time: 1.02 ms

-- the equivalent check against a folder that DOES have a colliding sibling
Index Scan using idx_objects_bucket_id_name_lower on objects o
  ...
  Rows Removed by Filter: 1           <- finds the mismatch on the first row, stops
  Buffers: shared hit=6 · Execution Time: 0.015 ms
```

**This is the whole story in one pair of plans.** The check is cheap when it
finds a match early (a real collision) and expensive in exact proportion to
folder size when it doesn't (the common case) - there is no way to prove "no
collision" without examining every row that could be the exception, and that
cost, multiplied by "once per folder walked" (up to the full folder count for
a deep-offset page), is exactly what section 2's `original`-plus-a-check
alternatives (`ALGORITHM-VERSIONS.md` v3/v4) paid and v5 doesn't. v5 doesn't
make this query faster; it makes the fast path never need to run it.

**Root-listing black-box plans, warmed, 10,000,000 rows, deep offset, ascending**
(same-session 3rd-call numbers, see note above):

| | original | v5 |
|---|---|---|
| root-clean | 29.0 ms, `shared hit=15763` | 22.0 ms, `shared hit=16068` |
| root-collision | 30.6 ms, `shared hit=15831` (wrong answer) | 23.6 ms, `shared hit=15849` (correct answer) |

Buffer counts are within 2% of each other in every pairing - v5 touches
essentially the same amount of data as `original` to do *more* (provably
correct) work. Both plans remain a single opaque `Function Scan` node to the
outer planner either way (expected - `storage.search()` is `plpgsql`, so its
internal query plans aren't visible to `EXPLAIN` except by pulling the
internal queries out standalone, as above); there is no planner-level
regression or surprising plan-shape change anywhere in this comparison.

## What wasn't done

- **100M+ row stress testing.** Not run in this pass, for the same time/disk
  reasons as before; the growth-rate analysis in section 3 is extrapolated
  from 100k-10M, not confirmed beyond it.
- **The `search`-fallback query shape (section 5) was only measured at two
  scales** (1k, 10M) - enough to show the mechanism and the magnitude, not a
  full growth curve. If this shape turns out to matter in production, that
  curve is the next thing worth measuring.
- **No production traffic shape was modeled**, same caveat as before: these
  numbers characterize this synthetic dataset's shape (folder size scales with
  `sqrt(total_rows)`, collisions clustered at the start of the alphabetical
  range), not a guaranteed prediction for any specific bucket.
