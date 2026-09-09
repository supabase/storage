# storage.search() case-collision fix - findings

Compares two versions of `storage.search()` (list v1's NAME-SORT path):
**before** - `storage.search()` as it stands on this branch's base
(`tyler/feat/object-versioning-wave-2`), and **after** - the same function with
this commit's fix applied. Both are version-aware (`noncurrent_versions`,
`delete_markers`); that logic is untouched by this change.

## The bug

`storage.search()` sorted and seeked by `lower(name)`. Two objects differing
only by case at some path segment (`my_Folder/x.png` vs `my_folder/x.png`) tie
on that key, and the function's O(1) "jump past this folder" step can't tell
them apart - it jumps past both, so the second one silently never appears in a
listing.

**The fix:** walk `name COLLATE "C"` directly instead. Two case-variant
folders are then just two different strings, never forced to tie, so a folder
jump can't merge them - no check needed. `prefix`/`search` still need
case-insensitive matching; that's resolved once per call instead of on every
comparison (see the migration's comments for the mechanism). The one shape
that can't resolve to a single boundary - `search` ending mid-segment, e.g.
`search='F'` matching sibling folders `FOO`/`Foo`/`foo` - falls back to a
per-folder check-and-resolve, applied to the previous algorithm. No new
tables, triggers, or indexes.

## Accuracy

Before returns fewer rows than actually exist whenever a listed range
contains a case collision - confirmed directly, same dataset, both versions:

| scenario                                                            | rows expected | before                              | after     |
| ------------------------------------------------------------------- | ------------- | ----------------------------------- | --------- |
| 100k-row dataset, 16 case-collision pairs under `stress_collision/` | 332 folders   | **316** (16 twins silently dropped) | **332**   |
| 1M-row dataset, 50 case-collision pairs, `search='folder_'`         | 1,050         | **1,000** (50 twins dropped)        | **1,050** |

Not a rare edge case - it fires for any two names sharing a lowercased
segment, not just deliberate `Foo`/`foo` collisions.

## Performance

Root-level folder listing, sorted by `name`, averaged over 5 warmed repeats.
`root-clean` = no collisions in that subtree; `root-collision` = a real
collision every ~20 folders (before returns wrong answers here - included to
show it's not "slow because correct", it's just slow).

| rows       | offset        | root-clean before | root-clean after | root-collision before | root-collision after |
| ---------- | ------------- | ----------------- | ---------------- | --------------------- | -------------------- |
| 100,000    | deep (~206)   | 2.85 ms           | 2.07 ms          | 2.67 ms               | 2.03 ms              |
| 1,000,000  | deep (~890)   | 12.02 ms          | 9.26 ms          | 9.82 ms               | 7.48 ms              |
| 10,000,000 | deep (~3,052) | 31.54 ms          | 26.59 ms         | 30.44 ms              | 31.13 ms             |

`after` matches or beats `before` at every scale tested, on both clean and
colliding data - fixing the bug did not cost anything on this path. (10M
`root-collision` is within noise of `before`, not a regression; see raw CSVs
for the full offset/direction matrix if reproducing this.)

**The one real cost - `search` ending mid-segment** (the fallback path):

| rows      | before                            | after              | why                                                               |
| --------- | --------------------------------- | ------------------ | ----------------------------------------------------------------- |
| 1,000,000 | 1.20 ms (wrong: 1,000/1,050 rows) | 61.56 ms (correct) | per-folder existence check, can't short-circuit the negative case |

This is the accuracy/performance trade the fix makes for this one query
shape: correctness costs roughly 50x here at 1M rows, because proving "no
collision" for a folder that doesn't have one requires scanning every row in
it - there's no cheaper way to prove a negative without new schema. This
shape is not used by plain folder navigation (empty or delimiter-terminated
`prefix`), only by a partial `search` filter, and every other measurement in
this document is unaffected by it.

## EXPLAIN ANALYZE - why

**After's fast-path peek** (`idx_objects_bucket_id_name`, exact order):
index-only scan, 4 buffer hits, 0.16ms, `Heap Fetches: 0` - same shape and
cost as before's `lower(name)`-ordered peek (`idx_objects_bucket_id_name_lower`),
just keyed on `name` instead. Neither peek query changed in kind; this is why
the happy path is unaffected.

**The fallback's existence check** (100k-row scale, a real, non-colliding
316-file folder):

```
Index Scan using idx_objects_bucket_id_name_lower on objects o
  Index Cond: (bucket_id = ... AND lower(name) >= 'stress/folder_0000001/' AND lower(name) < 'stress/folder_00000010')
  Filter: (archived_at IS NULL AND NOT is_delete_marker AND left(name, 22) <> 'stress/folder_0000001/')
  Rows Removed by Filter: 316        <- scans the whole folder to prove no collision
  Buffers: shared hit=20 · Execution Time: 0.31 ms
```

Cheap when a collision is real (stops at the first mismatch); this is the
common case - no collision - so it can't stop early, and cost scales with
folder size. That's the entire mechanism behind the one number in this
document that's worse than before: it's not new, it's the same cost the old
per-call check would have paid for _every_ folder walked, now paid only for
this one query shape instead of on every listing call.

## Sorting by other columns

Unaffected - `updated_at`/`created_at`/`last_accessed_at` sorting uses a
separate `path_tokens`/`ILIKE` path this fix doesn't touch, before or after.
