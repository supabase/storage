import type { DatabaseTransaction, DatabaseTransactionalExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { PgShardStoreFactory, ShardCatalog } from '@internal/sharding'
import { DatabaseError } from 'pg'
import { icebergResourceLockKey } from '../resource-lock'
import { IcebergError, IcebergErrorType } from './errors'
import type { RestCatalogClient } from './rest-catalog-client'

export const RECLAIM_BATCH_SIZE = 100

export interface ReclaimShardSlotsOptions {
  runId?: string
  dryRun?: boolean
  tenantId?: string
  shardId?: string
  afterReservationId?: string
}

interface Candidate {
  id: string
  shard_id: string
  slot_no: number
  tenant_id: string
  resource_id: string
  shard_key: string
}

type Outcome =
  | 'reclaimed'
  | 'reclaimable'
  | 'localTable'
  | 'upstreamTable'
  | 'changed'
  | 'unsupported'
  | 'skipped'

// Both historical catalog-name and current catalog-ID keys contain the namespace UUID.
const resourcePattern =
  /^iceberg-table::.+::([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\/([^/]+)$/i

export class IcebergShardSlotReclaimer {
  constructor(
    private readonly db: DatabaseTransactionalExecutor,
    private readonly catalog: Pick<
      RestCatalogClient,
      'tableExists' | 'loadTable' | 'listNamespaces'
    >
  ) {}

  async runBatch(options: ReclaimShardSlotsOptions) {
    const verifiedShards = new Set<string>()

    const { rows } = await this.db.query<Candidate>({
      text: `
        SELECT r.id, r.shard_id::text, r.slot_no, r.tenant_id, r.resource_id, s.shard_key
        FROM public.shard_reservation r
        JOIN public.shard s ON s.id = r.shard_id AND s.kind = 'iceberg-table'
        JOIN public.shard_slots sl ON sl.shard_id = r.shard_id AND sl.slot_no = r.slot_no
          AND sl.tenant_id = r.tenant_id AND sl.resource_id = r.resource_id
        WHERE r.kind = 'iceberg-table' AND r.status = 'confirmed'
          AND ($1::text IS NULL OR r.tenant_id = $1)
          AND ($2::bigint IS NULL OR r.shard_id = $2)
          AND r.id > $3::uuid
        ORDER BY r.id
        LIMIT $4
      `,
      values: [
        options.tenantId ?? null,
        options.shardId ?? null,
        options.afterReservationId ?? '00000000-0000-0000-0000-000000000000',
        RECLAIM_BATCH_SIZE,
      ],
    })

    const counts: Record<Outcome, number> = {
      reclaimed: 0,
      reclaimable: 0,
      localTable: 0,
      upstreamTable: 0,
      changed: 0,
      unsupported: 0,
      skipped: 0,
    }
    const reclaimedReservationIds: string[] = []
    for (const candidate of rows) {
      const outcome = await this.inspect(candidate, options.dryRun !== false, verifiedShards)
      counts[outcome]++
      if (outcome === 'reclaimed' || outcome === 'reclaimable') {
        reclaimedReservationIds.push(candidate.id)
      }
      if (outcome === 'reclaimed') {
        // Keep an audit record even if a later candidate fails and the batch is retried.
        logSchema.info(logger, '[Iceberg] Shard allocation reclaimed', {
          type: 'iceberg-shard-reclamation',
          project: candidate.tenant_id,
          metadata: JSON.stringify({ runId: options.runId, ...candidate }),
        })
      }
    }

    return {
      scanned: rows.length,
      ...counts,
      reclaimedReservationIds,
      nextAfterReservationId:
        rows.length === RECLAIM_BATCH_SIZE ? rows[rows.length - 1].id : undefined,
    }
  }

  private async inspect(
    candidate: Candidate,
    dryRun: boolean,
    verifiedShards: Set<string>
  ): Promise<Outcome> {
    const match = resourcePattern.exec(candidate.resource_id)
    if (!match || !candidate.tenant_id) return 'unsupported'
    const namespaceId = match[1].toLowerCase()
    const tableName = match[2]

    // Validate the endpoint without holding writer locks. A warehouse 404 is not
    // evidence of table absence; the table check still runs under the locks below.
    if (!verifiedShards.has(candidate.shard_key)) {
      await this.catalog.listNamespaces({ warehouse: candidate.shard_key, pageSize: 1 })
      verifiedShards.add(candidate.shard_key)
    }

    const tnx = await this.db.beginTransaction()
    try {
      await tnx.query("SET LOCAL lock_timeout = '2s'")
      await tnx.query("SET LOCAL statement_timeout = '5s'")
      // Bypass PgMetastore's error mapping so 55P03/57014 reach the skip handler.
      const store = new PgShardStoreFactory(tnx).autocommit()
      for (const key of [
        icebergResourceLockKey('namespace', `${candidate.tenant_id}:${namespaceId}`),
        candidate.resource_id,
      ]) {
        await store.advisoryLockByString(key)
      }

      const outcome = await this.inspectLocked(tnx, candidate, namespaceId, tableName, dryRun)
      await tnx.commit()
      return outcome
    } catch (error) {
      try {
        await tnx.rollback()
      } catch (rollbackError) {
        if (rollbackError instanceof Error) rollbackError.cause = error
        throw rollbackError
      }
      if (error instanceof DatabaseError && (error.code === '55P03' || error.code === '57014')) {
        return 'skipped'
      }
      throw error
    }
  }

  private async inspectLocked(
    tnx: DatabaseTransaction,
    candidate: Candidate,
    namespaceId: string,
    tableName: string,
    dryRun: boolean
  ): Promise<Outcome> {
    // A delayed job must never free a slot that has been reused since discovery.
    const current = await tnx.query({
      text: `
        SELECT r.id
        FROM public.shard_slots sl
        JOIN public.shard_reservation r ON r.shard_id = sl.shard_id AND r.slot_no = sl.slot_no
        WHERE r.id = $1::uuid AND r.status = 'confirmed'
          AND r.resource_id = $4 AND r.tenant_id = $5
          AND sl.shard_id = $2::bigint AND sl.slot_no = $3
          AND sl.resource_id = $4 AND sl.tenant_id = $5
        FOR UPDATE OF sl, r
      `,
      values: [
        candidate.id,
        candidate.shard_id,
        candidate.slot_no,
        candidate.resource_id,
        candidate.tenant_id,
      ],
    })
    if (current.rows.length === 0) return 'changed'

    // Keep allocations for any matching local table, including legacy name-keyed records.
    const local = await tnx.query({
      text: `
        SELECT 1 FROM public.iceberg_tables
        WHERE tenant_id = $1 AND namespace_id = $2::uuid AND name = $3
        LIMIT 1
      `,
      values: [candidate.tenant_id, namespaceId, tableName],
    })
    if (local.rows.length > 0) return 'localTable'

    const table = {
      warehouse: candidate.shard_key,
      namespace: `${candidate.tenant_id}_${namespaceId.replaceAll('-', '_')}`,
      table: tableName,
    }
    try {
      await this.catalog.tableExists(table)
      return 'upstreamTable'
    } catch (error) {
      if (!isMissingTableError(error)) throw error
    }

    // HEAD has no error body: a proxy 404 looks like a missing table. Confirm
    // absence with a structured catalog GET 404 while still holding writer locks.
    try {
      await this.catalog.loadTable(table, { requireStructuredErrors: true })
      return 'upstreamTable'
    } catch (error) {
      if (!isMissingTableError(error)) throw error
    }

    if (dryRun) return 'reclaimable'
    await new ShardCatalog(new PgShardStoreFactory(tnx)).freeByLocation(
      candidate.shard_id,
      candidate.slot_no
    )
    return 'reclaimed'
  }
}

function isMissingTableError(error: unknown): error is IcebergError {
  return (
    error instanceof IcebergError &&
    error.code === 404 &&
    (error.type === IcebergErrorType.NoSuchTableException ||
      error.type === IcebergErrorType.NoSuchNamespaceException)
  )
}
