# Iceberg Protocol Bug Fixes

## 1. Knex Pool Deadlock in `updateTable`

**File:** `src/storage/protocols/iceberg/catalog/tenant-catalog.ts` (lines 339, 345)

**Symptom:** `KnexTimeoutError: Knex: Timeout acquiring a connection. The pool is probably full.` when committing data to a table (e.g. `table.append(df)` from pyiceberg).

**Root cause:** Inside `updateTable`, a transaction is opened via `this.options.metastore.transaction(async (store) => { ... })`. The transaction callback receives `store` — a `KnexMetastore` instance bound to the transaction's connection. However, `findNamespaceByName` and `findTableByName` were called on `this.options.metastore` (the root instance) instead of `store`, causing them to acquire a **new** connection from the pool. With a small pool (typical in single-tenant dev), this deadlocks: the transaction holds the only connection while the inner queries wait for a free one.

**Fix:** Use `store` (the transactional instance) instead of `this.options.metastore`:

```diff
- const namespace = await this.options.metastore.findNamespaceByName({ ... })
+ const namespace = await store.findNamespaceByName({ ... })

- const dbTable = await this.options.metastore.findTableByName({ ... })
+ const dbTable = await store.findTableByName({ ... })
```

**Reference:** `createTable` already follows the correct pattern — all queries inside its transaction use `store`.

---

## 2. Shard Key Falsiness Check Breaks Single-Tenant Mode

**File:** `src/storage/protocols/iceberg/catalog/tenant-catalog.ts` (lines 311, 351, 391, 530)

**Symptom:** `ShardNotFound: Table shard key not found for table <name>` on `loadTable`, `updateTable`, `tableExists`, and `dropTable` — even though the table was successfully created.

**Root cause:** In single-tenant mode, sharding is not used. The `SingleShard` strategy returns `shardKey: ''` (empty string) from `reserve()`, derived from `ICEBERG_WAREHOUSE` which is intentionally empty (the underlying `tabulario/iceberg-rest` catalog doesn't support warehouse prefix routing). The empty string is a valid shard key — it means "no warehouse prefix."

However, all four guard checks used JavaScript falsiness (`!dbTable.shard_key`), which rejects empty string along with `null`/`undefined`:

```typescript
if (!dbTable.shard_key) {
  throw ERRORS.ShardNotFound(...)
}
```

**Fix:** Check for `null`/`undefined` only, allowing empty string through:

```diff
- if (!dbTable.shard_key) {
+ if (dbTable.shard_key == null) {
```

Applied to `loadTable`, `updateTable`, `tableExists`, and `dropTable`.

---

## 3. Empty Warehouse Causes Double-Slash URLs

**File:** `src/storage/protocols/iceberg/catalog/rest-catalog-client.ts`

**Symptom:** Requests to the underlying catalog had URLs like `//namespaces` instead of `/namespaces`, causing 400 errors.

**Root cause:** `getEncodedWarehouse('')` returned `'/'` when the warehouse was empty, and this was prepended to paths like `/namespaces`.

**Fix:** Return empty string when warehouse is empty:

```typescript
getEncodedWarehouse(warehouse: string) {
  if (!warehouse) {
    return ''
  }
  return '/' + encodeURIComponent(warehouse)
}
```
