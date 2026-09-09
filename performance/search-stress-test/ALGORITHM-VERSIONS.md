# storage.search() algorithm versions

What each benchmarked version of `storage.search` actually does internally, so the
numbers in `FINDINGS.md` can be traced back to a specific mechanism instead of just
a label. Written for whoever picks this up next to try a different approach -
know what you're comparing against before proposing something new.

`storage.search` has a longer history than what's covered here (it's been
rewritten several times since the original 2024 version - see `git log --oneline
-- migrations/tenant/*search*.sql` if you want the full lineage). This document
starts at the version that was live immediately before this round of work, since
that's the actual baseline everything in `FINDINGS.md` is measured against.

Both versions below share the same overall shape and only differ in the "NAME
SORTING" section covered here - the "NON-NAME SORTING" branch (`ILIKE` + `GROUP
BY`, used for `created_at`/`updated_at`/`last_accessed_at`) is byte-identical
between them and isn't part of this document (see FINDINGS.md's closing section on
that).

## v1 - pre-fix baseline

`migrations/tenant/0063-fix-search-name-relative-to-prefix.sql` as of commit
`8aa704af5c71bc7fc1cfd40aeaf33fd2fad51991` (2026-08-20, PR #1339) - extracted via
`git show 8aa704af5c7:migrations/tenant/0063-fix-search-name-relative-to-prefix.sql`
(see the README's "Testing against older function versions" section), and what
every "pre-fix" column in `FINDINGS.md` measured.

**Sort/seek key:** `lower(name) COLLATE "C"` alone, for both the peek query
(single-row lookup that drives the main loop) and the batch query (multi-row file
fetch). One key, no tiebreaker.

**Folder-boundary handling:** when the peek lands on a row that's inside a
subfolder (`storage.get_common_prefix(lower(peek_name), lower(prefix), '/')` is
non-null), it emits that folder once, then jumps the seek cursor straight to
`lower(left(common_prefix, -1)) || chr(ascii('/') + 1)` - i.e. past the *entire*
range of anything whose **lowercased** name starts with that folder's prefix, in
a single O(1) step. No check of any kind on what else might be in that range.

**What this means:**
- **Fast.** One index seek per folder boundary, full stop - this is why every
  "pre-fix" number in FINDINGS.md is so much smaller.
- **Wrong when two folders differ only by case.** `my_Folder` and `my_folder`
  produce the identical lowercased jump target, so the second one's rows are
  silently skipped over as if they were part of the first - the two folders merge
  into whichever one's name the peek happened to land on first. This is the
  correctness bug the fix (v3 below) addresses.
- **Separately, non-deterministic under pagination even without case collisions.**
  With only `lower(name)` as the sort key, two *different* files that happen to
  share a lowercased name-prefix-up-to-tie-break-point have no defined relative
  order - `ORDER BY ... LIMIT 1` is free to return either on any given call, and
  offset-based pagination (independent `LIMIT`/`OFFSET` calls) has no guarantee
  it picks the same one twice. Not exercised directly in FINDINGS.md's numbers,
  but discovered and confirmed during this work - see the migration's git history
  for the repro.
- **Also has a separate, still-unfixed bug**, unrelated to case sensitivity: in
  `DESC` order, if a file-batch scan crosses directly into a folder boundary
  mid-batch, that folder can be silently dropped from the results entirely (not
  merged - just missing). Confirmed present in both this version and the current
  one (this document's v3) - it was out of scope for the collision fix and hasn't
  been touched. Worth its own investigation separately.

## v2 - interim attempt (not benchmarked, mentioned for completeness only)

Committed as `7b7b01d40c5f7c083e46d724840b33b715e266ce` ("DO NOT MERGE - this
branch is just to show the diff"), superseded before any numbers in this folder
were gathered against it. Included here only so it doesn't get reinvented.

**What it tried:** keep the single `lower(name)` sort key, but before taking the
v1-style O(1) jump past a folder, run one query to look for the next row whose
exact-case prefix differs from the one just emitted, and if found, re-seek to
exactly that row instead of jumping past the whole range.

**Why it was replaced:** the reseek target was computed with `>=`/`<`
(inclusive/exclusive) against the *same* `lower(name)` key being tied on. If two
differently-cased folders both contained a file with the identical relative name,
their `lower(name)` values were byte-identical - the reseek could not distinguish
"already emitted" from "not yet emitted" and kept re-selecting the same row,
re-emitting one folder repeatedly until it hit the row limit while the sibling
folder was never reached. A single-row reseek fundamentally can't resolve ties on
the same key it's seeking by; you need either a compound key or a set-based
approach - which is what v3 does.

## v3 - per-call existence check (superseded by v5, see below)

`migrations/tenant/0063-fix-search-name-relative-to-prefix.sql` as it stands in
the repo right now.

**Sort/seek key:** `lower(name) COLLATE "C", name COLLATE "C"` - two keys. The
first preserves v1's case-insensitive ordering; the second makes every comparison
deterministic (fixes the pagination non-determinism noted under v1, independent of
collisions). Applied to *every* peek and batch query, not just the folder path -
this is why even `within-folder` (pure file listing, never touches collision
logic) shows a small but real cost increase over v1 in FINDINGS.md - it's the
price of the second sort key, paid everywhere, not of collision handling
specifically.

**Folder-boundary handling:** replaces v1's unconditional jump with:

1. Compute the same lowercased range v1 would have jumped past (`v_range_lo` /
   `v_range_hi`).
2. **Existence check:** `SELECT EXISTS (... WHERE lower(name) is in that range AND
   exact-case prefix differs from the one just emitted ...)`. If this finds
   nothing, take v1's original O(1) jump - the common case is exactly as fast as
   v1 modulo the existence check itself.
3. **Only if a collision is found:** run a second query - `SELECT DISTINCT
   get_common_prefix(name, ...) ... ORDER BY ... ASC/DESC` over that same range -
   to enumerate every distinct exact-case folder name present, and emit all of
   them (the one already emitted in step 1 is excluded from this second query so
   it isn't duplicated).

**What this means:**
- **Correct.** Every distinct exact-case folder is emitted exactly once,
  regardless of how many differently-cased siblings share the same lowercased
  name, including the exact-tie case that broke v2.
- **The existence check in step 2 is only cheap when it finds a match early.**
  When the true answer is "no collision" (by far the common case), `EXISTS` still
  has to examine every row in that folder's range before it can conclude nothing
  matches - there's no way to prove a negative without checking everything. This
  is the mechanism behind essentially all of the scaling behavior documented in
  `FINDINGS.md` section 1-2: the check's cost is proportional to how many rows are
  in the folder being checked, and it runs once per folder walked, so total added
  cost scales with **total rows touched during the walk**, not just folder count.
  See `FINDINGS.md` and `sql/20_explain.sql` (#12) for the measured numbers.
- **Step 3's resolution query is a separate, smaller cost**, proportional to the
  size of the specific colliding folder(s) - only paid when a collision genuinely
  exists. See `FINDINGS.md` section 2's second table and `sql/20_explain.sql`
  (#13).
- **Deliberately minimal-diff against v1** for the non-colliding path: steps 1-2
  are pure additions after what v1 already did (emit the folder, same as before);
  step 3 only runs conditionally. This was a deliberate structural choice (see PR
  discussion) to keep the well-tested common path recognizable rather than
  routing every call through shared machinery - it does mean the "cheap path" and
  "collision path" are two separate code blocks rather than one unified one, which
  is worth knowing if refactoring this further.

## v4 - bucket-flag gated (considered, superseded by v5 - kept for the record)

Explored and benchmarked, then replaced before shipping: it needed a new table,
two new triggers, and a write-path cost to get there, and even then it had a
real weak spot (see "the bucket-wide granularity is the real tradeoff" below).
v5 gets equal-or-better numbers with none of that - see v5's entry for why this
approach turned out not to be necessary. Left in place as a record of what was
tried and why it wasn't the final answer, not as a description of what's
currently in `migrations/tenant/0063-fix-search-name-relative-to-prefix.sql`.

Structurally v3's algorithm, unchanged, but the existence check + resolution
steps only run when
`storage.buckets.name_case_collision` is true for the bucket being listed.

**The insight this is built on:** v3's cost problem (section 2 of FINDINGS.md) is
that *proving a negative requires a scan*, and that scan re-runs once per folder
walked, on every call. Neither half of that is fixable inside a single
`storage.search()` invocation - there's no way to prove "no collision in this
range" cheaper than checking every row in it. But the *answer* to "does this
bucket contain a case collision anywhere" doesn't change between calls nearly as
often as `storage.search()` is called - inserts that introduce a new
distinctly-cased folder are rare; listings of a large bucket are not. So v4 moves
the expensive question out of the read path entirely: answer it once, when the
colliding object is written, and cache the answer as a boolean on
`storage.buckets`, updated in place. Every `storage.search()` call after that
is a single indexed point lookup (`storage.buckets` PK, not a scan), not a
per-folder existence check - see the `EXPLAIN` in the "measured" section below.

**Sort/seek key:** `lower(name) COLLATE "C", name COLLATE "C"` - same as v3,
applied unconditionally. The secondary key isn't part of the collision fix (it
fixes a different, independent pagination-determinism bug - see v3's entry
above) and is cheap enough (FINDINGS.md's "what's already good" section) that
there's no reason to gate it.

**Folder jump:** identical to v3's when `name_case_collision` is true for the
bucket. When it's false - the common case, checked once per `search()` call, not
once per folder - skips straight to v1's original unconditional O(1) jump, with
no existence check at all.

**What maintains the flag:** `storage.check_prefix_case_collisions()`, a
*statement-level* `AFTER INSERT` trigger on `storage.objects` (using a `NEW
TABLE` transition table, so a single 200k-row bulk insert is examined once as a
set, not fired 200k times). For every ancestor folder prefix introduced by the
batch, it registers `(bucket_id, lower(prefix)) -> exact_prefix` into a new small
table, `storage.prefix_case_index`, and flips `name_case_collision` to true the
moment two different exact prefixes ever land on the same lowered key - whether
that happens within one batch or across two separate inserts. Cost is
proportional to the number of *distinct* ancestor folders touched by a write, not
to how many rows are already in them - the opposite scaling of the check it
replaces. `prefix_case_index` is append-only (no delete-side cleanup): a stale
row can only cause a false positive (a bucket stays on the safe path after its
one colliding folder is later deleted), never a false negative, which is the
only direction that would matter for correctness. That asymmetry is what lets
this avoid the delete-triggered bookkeeping that made the old `storage.prefixes`
table (removed in migrations 0050/0052, see that table's own history for why)
fragile under concurrent writes - this design deliberately doesn't attempt what
that table did.

A one-time backfill in the same migration seeds `prefix_case_index` and the
buckets flag from whatever already exists in `storage.objects`, since the
trigger only sees future writes.

**What this doesn't cover:** an object *rename* (`UPDATE ... SET name`) that
introduces a new collision isn't tracked incrementally - only `INSERT` is
wired up. A rename can only ever move a bucket from "flagged" toward
"un-flagged-but-actually-safe" or leave it exactly as collision-prone as
before, never introduce a *new* collision that a rename didn't already require
inserting through at some point - except a rename that shortens a name across a
delimiter boundary, which is a real (if narrow) gap worth closing before this
goes further than a prototype.

**Measured** (see `results/run-cleanonly1m-*`, `results/run-cleanonly10m-*`,
`results/run-shared1m-*`): root listing, sorted by `name`, deep offset, averaged
over 5 repeats.

On a bucket with **zero collisions anywhere** (`name_case_collision = false`,
the common case):

| objects | v1 (pre-fix) | v3 (current) | v4 (bucket-flag) | v4 vs v1 | v4 vs v3 |
|---|---|---|---|---|---|
| 1,000,000  | 7.5ms  | 353-362ms   | 9.3-9.6ms | ~1.25x slower | **~37x faster** |
| 10,000,000 | 24.7-26.5ms | 3,400-3,675ms | 32.1-32.2ms | ~1.25x slower | **~110x faster** |

On the shared bucket used elsewhere in this harness (`stress_collision/` has a
real collision, so `name_case_collision = true` for the *whole bucket*,
including the unrelated `stress/` subtree) at 1,000,000 objects: v4 tracks v3
within noise in both the `root-clean` (350.8ms vs 358.2ms) and `root-collision`
(417.6ms vs 449.6ms) scenarios - i.e. no regression versus v3 once the safe path
is actually needed, at the cost of one extra buckets-table lookup per call.

**Write-path cost:** seeding 2,050,000 rows (the `perf-stress-test` shared
bucket, trigger doing real work on every batch) took 52.2s and 33.5s across two
runs; the same seed with the trigger disabled took 62.6s and 77.4s. The
trigger's cost is not distinguishable from run-to-run variance at this scale -
consistent with the design goal (cost proportional to distinct folders per
batch, not rows), though this is a workstation-scale, not a production-scale,
measurement.

**The bucket-wide granularity is the real tradeoff.** The flag is per-bucket,
not per-prefix: a single colliding pair anywhere in a bucket - even under a
completely unrelated top-level prefix, as the shared-bucket measurement above
shows - puts every listing call against that bucket on the slow path, forever
(the flag is one-way). For a bucket holding one tenant's files that's a
non-issue; for a bucket shared across many logically-separate prefixes (e.g. one
bucket per app, many users' files under per-user prefixes), one user's
colliding upload slows down listing for every other prefix in that bucket. That
tradeoff - coarser granularity in exchange for the check being O(1) instead of
O(rows) - is the one this version makes; a per-prefix or per-folder flag would
close that gap but couldn't be a single indexed point lookup anymore, which is
what makes this version cheap.

## v5 - exact-order walk (adopted, current)

`migrations/tenant/0063-fix-search-name-relative-to-prefix.sql` as it stands in
the repo right now. No new tables, no triggers, no stored flags - the schema is
byte-for-byte what migration 0020 already left behind
(`idx_objects_bucket_id_name` on `(bucket_id, name COLLATE "C")`). This is the
one that shipped.

**The question that led here: why was the sort/seek key `lower(name)` at all?**
Traced through the migration history, not a deliberate "sort case-insensitively"
design choice - migration 0056's own message is *"fixes issue where prefixes are
returned in lowercase for search v1"*. `prefix`/`search` need to match
case-insensitively (the "case insensitive search should work" test), and once
the WHERE bound is `lower(name)`-based, using the same expression for `ORDER BY`
was the only way to get an index-backed walk. The sort order piggybacked on the
filter. That's *why* v1 through v4 all forced two case-variant siblings to tie
on the primary sort key - and every fix from v2 through v4 is, in one way or
another, working around the consequences of that tie. `storage.search_v2`
/`list_objects_with_delimiter` (migration 0050) already sorts and seeks purely
by `name COLLATE "C"` and has never had this bug, by construction: distinct
exact strings are never forced to tie, so there's nothing to prove and nothing
to check.

**Sort/seek key:** `name COLLATE "C"` alone. No second tie-break key needed
either - `name` is already unique per bucket (`bucketid_objname`), so unlike
`lower(name)` it can never tie; deterministic pagination (the bug v3's second
sort key fixed) falls out for free.

**Folder jump:** the original v1 O(1) unconditional jump, always, for every
folder boundary, full stop. Two case-variant folders are two different strings
under `name COLLATE "C"` ordering - neither is ever mistaken for a range
containing the other, so there is nothing to check. This is the core difference
from v3 and v4: not a cheaper check, no check at all.

**The catch, and how it's handled:** `prefix`/`search` still need to match
case-insensitively, and the exact-order walk alone can't do that (a
case-insensitive match isn't a contiguous range in exact-name order). Splitting
this in two turned out to matter:
- **`prefix` (empty or ends at a delimiter - i.e. every plain "list this
  folder" call):** resolved *once*, before the walk starts, not on every
  comparison during it. Try the caller's literal bytes first (one existence
  check bounded exactly like the rest of the walk) - the common case, since a
  client normally got this prefix from a prior listing and already has the
  right case. Only if that finds nothing does it fall back to one
  `lower(name)`-indexed peek (`idx_objects_bucket_id_name_lower`, migration
  0051) to find the real casing - one query regardless of how large the
  matched subtree is, since it only needs *any* one matching row, not proof
  there's only one. Safe here specifically because the character being bumped
  for the exact-match attempt is the delimiter itself, which has no case
  variants, so "found nothing" and "found the wrong casing" are the only two
  outcomes.
- **`search` supplying a partial, non-delimiter-terminated suffix** (e.g.
  `search='F'` matching sibling folders `FOO`/`Foo`/`foo` - and, as discovered
  while getting the case-collision test suite fully green, *also* potentially
  matching unrelated folders like `Fantastic/` or `flower/` that merely share
  the prefix): "resolve once" doesn't apply, because there's no single boundary
  to resolve - a broad partial match can span several *different* folders, each
  of which independently might have its own collision. That's the same
  data-shaped, per-folder cost v3 always paid, no matter the approach, so this
  one query shape falls back to v3's algorithm verbatim (kept in the same
  function, gated on `search` producing a non-delimiter-terminated suffix - a
  property of the call's arguments, not of the data, so every call takes one of
  two well-defined, always-correct paths deterministically). This is a narrow
  shape: plain folder navigation (what real UIs do, and what every benchmark in
  this folder measures) never produces it.

**Measured** (see `results/run-cleanonly1m-*`, `results/run-cleanonly10m-*`,
`results/run-exactorder1m-*`): root listing, sorted by `name`, deep offset,
averaged over 5 repeats.

On a bucket with zero collisions anywhere:

| objects | v1 (pre-fix) | v3 | v5 (exact-order) | v5 vs v1 | v5 vs v3 |
|---|---|---|---|---|---|
| 1,000,000 | 6.7ms | 338-341ms | 6.0-6.7ms | **on par** | **~50x faster** |
| ~9,760,000 | 32-34ms | 6,500-6,870ms | 25.8-27.1ms | **~1.2x faster** | **~250x faster** |

More importantly, on the **shared bucket** used elsewhere in this harness
(`stress_collision/` has a real collision under an unrelated top-level prefix)
at 1,000,000 objects - the scenario that was v4's actual weak point:

| scenario | v1 | v3 | v5 |
|---|---|---|---|
| `root-clean` (unrelated to the collision) | 6.7ms | 338ms | **6.7ms** |
| `root-collision` (the colliding subtree itself) | 7.1ms | 413ms | **6.6ms** |

v5 matches v1's speed in **both** scenarios, in the same bucket, simultaneously
- there is no bucket-wide (or any other) blast radius, because there's no
stored state to poison. This is what v4 could not do: v4 measured 350.8ms /
417.6ms on this exact same shared-bucket scenario, because one collision
anywhere in the bucket put the whole bucket on the slow path. v5 has no such
tradeoff to make.

**Write-path cost:** none. Nothing changed about how objects are written.

## At a glance

| | v1 (pre-fix) | v2 (interim, unbenchmarked) | v3 | v4 (considered) | v5 (adopted) |
|---|---|---|---|---|---|
| Sort/seek key | `lower(name)` | `lower(name)` | `lower(name), name` | `lower(name), name` | `name` alone |
| New schema | none | none | none | table + 2 triggers + column | **none** |
| Folder jump (no collision) | O(1), unconditional | O(1) + 1 extra query | O(1) + 1 existence check | O(1) (flag lookup only) | **O(1), unconditional, always** |
| Folder jump (real collision) | O(1) - **wrong**, merges | reseeks row-by-row - **can loop forever** on exact ties | resolves all variants via 1 extra scan | same as v3 | same O(1) jump - nothing to resolve |
| Deterministic pagination | No | No | Yes | Yes | Yes (for free - name is unique) |
| Case-collision correctness | No (the bug) | No (different bug) | Yes | Yes | Yes |
| Cost of proving "no collision" | N/A (doesn't try) | N/A | O(rows in folder), every call | O(1) amortized to write time | **N/A - nothing to prove** |
| Granularity of any slow path | N/A | N/A | per-folder-walked | per-bucket (see v4's tradeoff) | per-call, and only for a `search` shape real navigation never produces |
| DESC file-batch-crosses-folder bug | Present | (not evaluated) | Present, unchanged | Present, unchanged | Present, unchanged |

## If you're trying a different approach

v5 is what shipped; if you're looking for something even better, the place to
push is the one thing it doesn't optimize: a `search` value ending mid-segment
still falls back to v3's per-folder existence check (see v5's entry above for
why "resolve once" doesn't cover that shape). That path is deliberately
untouched because plain folder navigation - empty or delimiter-terminated
`prefix`, the shape every benchmark here measures - never produces it, but if a
product surface leans on `search` for broad, partial-match filtering at scale,
that's the remaining O(rows)-in-the-worst-case corner. The DESC
file-batch-crosses-folder bug (present since v1, unrelated to case collisions)
is also still open.

Whatever you try, this harness (`./run.sh --function <path> --label <name>`)
will let you benchmark it against v1, v3, and v5 on the same data without
re-seeding - see the README's "Testing against older function versions"
section, and add a row to the table above once you know what it does
differently.
