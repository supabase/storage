import { Knex } from 'knex'
import {
  ReservationRow,
  ResourceKind,
  ShardRow,
  ShardStatus,
  ShardStore,
  ShardStoreFactory,
  UniqueViolationError,
} from './store'
import { hashStringToInt } from '@internal/hashing'

export class KnexShardStoreFactory implements ShardStoreFactory<Knex.Transaction> {
  constructor(private knex: Knex) {}

  withExistingTransaction(tnx: Knex.Transaction): ShardStoreFactory {
    return new KnexShardStoreFactory(tnx)
  }
  async withTransaction<T>(fn: (store: ShardStore) => Promise<T>): Promise<T> {
    if (this.knex.isTransaction) {
      // Already in a transaction, use current connection
      return fn(new KnexShardStore(this.knex))
    }

    try {
      return await this.knex.transaction(async (trx) => {
        return fn(new KnexShardStore(trx))
      })
    } catch (error) {
      throw error
    }
  }
  autocommit(): ShardStore {
    return new KnexShardStore(this.knex)
  }
}

class KnexShardStore implements ShardStore {
  constructor(private db: Knex | Knex.Transaction) {}

  private q<T = any>(sql: string, params?: any[]) {
    return this.db.raw<T>(sql, params as any)
  }

  async findShardById(shardId: number): Promise<ShardRow | null> {
    const shard = await this.db<ShardRow>('shard').select('*').where({ id: shardId }).first()
    return shard ?? null
  }

  async advisoryLockByString(key: string): Promise<void> {
    const id = hashStringToInt(key)
    await this.q(`SELECT pg_advisory_xact_lock(?::bigint)`, [id])
  }

  async findShardByResourceId(tenantId: string, resourceId: string): Promise<ShardRow | null> {
    const result = await this.db
      .select('s.shard_key', 's.id')
      .from('shard_slots as ss')
      .join('shard as s', 's.id', 'ss.shard_id')
      .where('ss.resource_id', resourceId)
      .where('ss.tenant_id', tenantId)
      .first()

    return result ?? null
  }

  async getOrInsertShard(
    kind: ResourceKind,
    shardKey: string,
    capacity: number,
    status: ShardStatus
  ): Promise<ShardRow> {
    const inserted = await this.db<ShardRow>('shard')
      .insert({ kind, shard_key: shardKey, capacity, status, next_slot: 0 })
      .onConflict(['kind', 'shard_key'])
      .ignore()
      .returning('*')
    if (inserted[0]) return inserted[0]
    const row = await this.db<ShardRow>('shard').where({ kind, shard_key: shardKey }).first()
    if (!row) throw new Error('Failed to fetch shard after idempotent insert')
    return row
  }

  async setShardStatus(shardId: string | number, status: ShardStatus): Promise<void> {
    await this.db('shard').update({ status }).where({ id: shardId })
  }

  async listActiveShards(kind: ResourceKind): Promise<ShardRow[]> {
    return this.db<ShardRow>('shard').select('*').where({ kind, status: 'active' })
  }

  async findShardWithLeastFreeCapacity(kind: ResourceKind): Promise<ShardRow | null> {
    const result = await this.q<{ rows: ShardRow[] }>(
      `
      WITH candidates AS (
        SELECT s.*,
               GREATEST(
                 (s.capacity - s.next_slot) +
                 COALESCE((
                   SELECT COUNT(*)
                   FROM shard_slots sl
                   WHERE sl.shard_id = s.id
                     AND sl.resource_id IS NULL
                     AND NOT EXISTS (
                       SELECT 1 FROM shard_reservation sr
                       WHERE sr.shard_id = sl.shard_id
                         AND sr.slot_no = sl.slot_no
                         AND sr.status = 'pending'
                         AND sr.lease_expires_at > now()
                     )
                 ), 0),
                 0
               ) AS free_capacity
        FROM shard s
        WHERE s.kind = ? AND s.status = 'active'
        FOR UPDATE
      )
      SELECT *
      FROM candidates
      WHERE free_capacity > 0
      ORDER BY free_capacity ASC, shard_key ASC
      LIMIT 1;
      `,
      [kind]
    )

    return result.rows[0] ?? null
  }

  async findReservationByKindKey(
    tenantId: string,
    kind: ResourceKind,
    resourceId: string
  ): Promise<ReservationRow | null> {
    return (
      (await this.db<ReservationRow>('shard_reservation')
        .select('shard_reservation.*', 'shard.shard_key as shard_key')
        // @ts-expect-error join column added dynamically
        .where({ 'shard_reservation.kind': kind, resource_id: resourceId })
        .andWhere('shard_reservation.tenant_id', tenantId)
        .join('shard', 'shard.id', 'shard_reservation.shard_id')
        .first()) ?? null
    )
  }

  async fetchReservationById(id: string): Promise<ReservationRow | null> {
    return (
      (await this.db<ReservationRow>('shard_reservation').select('*').where({ id }).first()) ?? null
    )
  }

  /**
   * Reserve one slot on a shard (single-table, no freelist):
   * 1) Try to claim a previously used-but-now-free row.
   * 2) If none, mint a fresh slot number by bumping shard.next_slot (bounded by capacity) and insert row.
   *
   * A slot is "free" if:
   * - resource_id IS NULL (not confirmed)
   * - AND no active pending reservation exists for it
   */
  async reserveOneSlotOnShard(shardId: string | number, tenantId: string): Promise<number | null> {
    // 1) Try to claim a free existing row
    const claimed = await this.q<{ rows: { slot_no: number }[] }>(
      `
      WITH pick AS (
        SELECT ss.slot_no
        FROM shard_slots ss
        WHERE ss.shard_id = ?
          AND ss.resource_id IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM shard_reservation sr
            WHERE sr.shard_id = ss.shard_id
              AND sr.slot_no = ss.slot_no
              AND sr.status = 'pending'
              AND sr.lease_expires_at > now()
          )
        ORDER BY ss.slot_no
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE shard_slots s
         SET tenant_id = ?
      FROM pick
      WHERE s.shard_id = ? AND s.slot_no = pick.slot_no
      RETURNING s.slot_no;
      `,
      [shardId, tenantId, shardId]
    )
    if (claimed.rows.length) return claimed.rows[0].slot_no

    // 2) Mint a fresh slot_no by bumping shard.next_slot (bounded by capacity)
    const minted = await this.q<{ rows: { slot_no: number }[] }>(
      `
      WITH ok AS (
        SELECT id, capacity, next_slot
        FROM shard
        WHERE id = ? AND status = 'active'
      ),
      bumped AS (
        UPDATE shard
           SET next_slot = ok.next_slot + 1
        FROM ok
        WHERE shard.id = ok.id
          AND ok.next_slot < ok.capacity
        RETURNING ok.next_slot AS slot_no
      )
      SELECT slot_no FROM bumped;
      `,
      [shardId]
    )
    const slotNo = minted.rows[0]?.slot_no ?? null
    if (slotNo == null) return null // at capacity or shard not active

    // Create the slot row
    try {
      await this.db('shard_slots').insert({
        shard_id: shardId,
        slot_no: slotNo,
        tenant_id: tenantId,
      })
      return slotNo
    } catch (e: any) {
      if (e?.code === '23505') {
        // Extremely rare race if another tx inserted the same slot first. Let caller try another shard/attempt.
        return null
      }
      throw e
    }
  }

  async insertReservation(data: {
    id: string
    kind: ResourceKind
    resourceId: string
    tenantId: string
    shardId: string | number
    shardKey: string
    slotNo: number
    leaseMs: number
  }): Promise<{ lease_expires_at: string }> {
    try {
      const row = await this.db('shard_reservation')
        .insert({
          id: data.id,
          kind: data.kind,
          resource_id: data.resourceId,
          tenant_id: data.tenantId,
          shard_id: data.shardId,
          slot_no: data.slotNo,
          status: 'pending',
          lease_expires_at: (this.db as any).raw(`now() + interval '${data.leaseMs} milliseconds'`),
        })
        .returning(['lease_expires_at'])

      return row[0]
    } catch (e: any) {
      if (e?.code === '23505') throw new UniqueViolationError()
      throw e
    }
  }

  /** Confirm atomically: pending+lease valid → mark slot resource_id + shard_reservation confirmed */
  async confirmReservation(
    reservationId: string,
    resourceId: string,
    tenantId: string
  ): Promise<number> {
    const res = await this.q(
      `
      WITH ok AS (
        SELECT r.shard_id, r.slot_no
        FROM shard_reservation r
        WHERE r.id = ?
          AND r.status = 'pending'
          AND r.tenant_id = ?
          AND r.lease_expires_at > now()
      ),
      upd_slots AS (
        UPDATE shard_slots s
           SET resource_id = ?
        FROM ok
        WHERE s.shard_id = ok.shard_id
          AND s.slot_no  = ok.slot_no
        RETURNING 1
      )
      UPDATE shard_reservation r
         SET status = 'confirmed'
      WHERE r.id = ?
        AND EXISTS (SELECT 1 FROM upd_slots)
      RETURNING 1;
      `,
      [reservationId, tenantId, resourceId, reservationId]
    )
    return (res as any).rowCount ?? (res as any).rows.length
  }

  async updateReservationStatus(
    id: string,
    status: 'confirmed' | 'cancelled' | 'expired'
  ): Promise<void> {
    await this.db('shard_reservation')
      .update({ status })
      .where({ id })
      .andWhere('status', '<>', status)
  }

  async deleteReservation(id: string): Promise<void> {
    await this.db('shard_reservation').where({ id }).del()
  }

  async deleteStaleReservationsForSlot(shardId: string | number, slotNo: number): Promise<void> {
    // Delete any old reservations for this slot (cancelled, expired, or confirmed)
    // This allows the slot to be reused with a new reservation
    await this.db('shard_reservation')
      .where({ shard_id: shardId, slot_no: slotNo })
      .where((q) => {
        q.whereIn('status', ['cancelled', 'expired']).orWhere((q2) => {
          q2.whereIn('status', ['pending', 'cancelled', 'expired']).andWhere(
            'lease_expires_at',
            '<',
            this.db.fn.now()
          )
        })
      })
      .del()
  }

  async loadExpiredPendingReservations(): Promise<ReservationRow[]> {
    return this.db<ReservationRow>('shard_reservation')
      .select('*')
      .where({ status: 'pending' })
      .andWhere('lease_expires_at', '<', this.db.fn.now())
  }

  async markReservationsExpired(ids: string[]): Promise<void> {
    if (!ids.length) return
    await this.db('shard_reservation').update({ status: 'expired' }).whereIn('id', ids)
  }

  async freeByLocation(shardId: string | number, slotNo: number): Promise<void> {
    // On delete of a confirmed resource, mark its row as reusable (clear resource_id and tenant_id)
    await this.q(
      `
      WITH shard_slots as (
        UPDATE shard_slots 
          SET resource_id = null, tenant_id = null
        WHERE shard_id = ? AND slot_no = ?
        RETURNING shard_id, slot_no
      ),
      deleted_reservations as (
        DELETE FROM shard_reservation
        WHERE shard_id = ? AND slot_no = ?
      )
      SELECT 1;
    `,
      [shardId, slotNo, shardId, slotNo]
    )
  }

  async freeByResource(shardId: string | number, resourceId: string, tenantId: string) {
    await this.q(
      `
      WITH shard_slots AS (
        UPDATE shard_slots
          SET resource_id = null, tenant_id = null
        WHERE shard_id = ? AND resource_id = ? AND tenant_id = ?
        RETURNING shard_id, slot_no
      ),
      deleted_reservations AS (
        DELETE FROM shard_reservation
        WHERE shard_id = ?
          AND resource_id = ?
          AND tenant_id = ?
      )
      SELECT 1;
    `,
      [shardId, resourceId, tenantId, shardId, resourceId, tenantId]
    )
  }

  async shardStats(kind?: ResourceKind) {
    const res = await this.q(
      `
      SELECT s.id AS shard_id, s.shard_key, s.capacity, s.next_slot,
             -- confirmed allocations
             (SELECT COUNT(*) FROM shard_slots sl WHERE sl.shard_id = s.id AND sl.resource_id IS NOT NULL) AS used,
             -- remaining capacity = (unused unminted capacity) + (existing free rows)
             GREATEST(
               (s.capacity - s.next_slot) +
               COALESCE((
                 SELECT COUNT(*)
                 FROM shard_slots sl
                 WHERE sl.shard_id = s.id
                   AND sl.resource_id IS NULL
                   AND NOT EXISTS (
                     SELECT 1 FROM shard_reservation sr
                     WHERE sr.shard_id = sl.shard_id
                       AND sr.slot_no = sl.slot_no
                       AND sr.status = 'pending'
                       AND sr.lease_expires_at > now()
                   )
               ), 0),
               0
             ) AS free
      FROM shard s
      ${kind ? `WHERE s.kind = ?` : ``}
      ORDER BY s.kind, s.shard_key;
      `,
      kind ? [kind] : []
    )

    return (res as any).rows.map((r: any) => ({
      shardId: String(r.shard_id),
      shardKey: r.shard_key,
      capacity: Number(r.capacity),
      used: Number(r.used),
      free: Number(r.free),
    }))
  }
}
