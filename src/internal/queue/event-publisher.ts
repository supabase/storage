import { Knex } from 'knex'
import { Semaphore } from '@shopify/semaphore'
import { Queue } from './queue'
import { getConfig } from '../../config'
import { logger, logSchema } from '@internal/monitoring'
import { getPostgresConnection, getServiceKeyUser } from '@internal/database'
import { verifyEventLogSignature } from './event-transaction'
import PgBoss from 'pg-boss'

const {
  eventLogBatchSize,
  eventLogConcurrency,
  eventLogPrefetchSize,
  eventLogPollIntervalMs,
  eventLogWarmPollDelaySeconds,
  eventLogSweepBatchSize,
  eventLogLeaseTimeoutSeconds,
  isMultitenant,
} = getConfig()

interface EventLogRow {
  id: string
  event_name: string
  payload: Record<string, unknown>
  send_options: Record<string, unknown> | null
  signature: string
  created_at: string
}

export class EventPublisher {
  private coldCursor: number = 0
  private semaphore: Semaphore
  private started = false
  private signal: AbortSignal | undefined

  constructor(private readonly knex: Knex) {
    this.semaphore = new Semaphore(eventLogConcurrency)
  }

  async start(signal: AbortSignal): Promise<void> {
    if (this.started) {
      return
    }
    this.started = true
    this.signal = signal

    logSchema.info(logger, '[EventLogProcessor] Starting', {
      type: 'event-log',
      metadata: JSON.stringify({
        batchSize: eventLogBatchSize,
        concurrency: eventLogConcurrency,
        prefetchSize: eventLogPrefetchSize,
        pollIntervalMs: eventLogPollIntervalMs,
        warmPollDelaySeconds: eventLogWarmPollDelaySeconds,
        leaseTimeoutSeconds: eventLogLeaseTimeoutSeconds,
      }),
    })

    // Start loops in background (resolve when aborted)
    void this.pollLoop()

    if (isMultitenant) {
      void this.sweepLoop()
    }
  }

  stop(): void {
    this.started = false

    logSchema.info(logger, '[EventLogProcessor] Stopped', {
      type: 'event-log',
    })
  }

  /**
   * Async loop that claims and processes tenants, then waits before the next cycle.
   * Backs off exponentially on consecutive failures to avoid hammering a struggling DB.
   */
  private async pollLoop(): Promise<void> {
    let consecutiveFailures = 0

    while (!this.signal?.aborted) {
      try {
        await this.pollRegisteredTenants()
        consecutiveFailures = 0
      } catch (e) {
        consecutiveFailures++
        logSchema.error(logger, '[EventLogProcessor] Error in poll loop', {
          type: 'event-log',
          error: e,
          metadata: JSON.stringify({ consecutiveFailures }),
        })
      }

      const delay = this.backoff(eventLogPollIntervalMs, consecutiveFailures)
      await this.sleep(delay)
    }
  }

  /**
   * Async loop for cold sweep. Runs less frequently (every 30s).
   * Backs off on consecutive failures.
   */
  private async sweepLoop(): Promise<void> {
    let consecutiveFailures = 0
    const baseInterval = 30_000

    while (!this.signal?.aborted) {
      const delay = this.backoff(baseInterval, consecutiveFailures)
      await this.sleep(delay)

      if (this.signal?.aborted) break

      try {
        await this.sweepColdTenants()
        consecutiveFailures = 0
      } catch (e) {
        consecutiveFailures++
        logSchema.error(logger, '[EventLogProcessor] Error in cold sweep', {
          type: 'event-log',
          error: e,
          metadata: JSON.stringify({ consecutiveFailures }),
        })
      }
    }
  }

  /**
   * Exponential backoff: base * 2^failures, capped at 60s.
   */
  private backoff(baseMs: number, failures: number): number {
    if (failures === 0) return baseMs
    return Math.min(baseMs * Math.pow(2, failures), 60_000)
  }

  private async pollRegisteredTenants(): Promise<void> {
    if (!isMultitenant) {
      // Single-tenant mode: process the single configured tenant directly
      const { tenantId } = getConfig()
      if (tenantId) {
        await this.processTenant(tenantId)
      }
      return
    }

    // Atomically claim tenants with FOR UPDATE SKIP LOCKED.
    // Sets next_poll_at to a lease timeout so other publishers skip them.
    // If this publisher crashes, the lease expires and another publisher picks them up.
    const claimed = await this.knex.raw<{ rows: { tenant_id: string }[] }>(
      `WITH claimed AS (
        SELECT tenant_id
        FROM event_log_tenants
        WHERE next_poll_at <= NOW()
        ORDER BY next_poll_at ASC
        LIMIT ?
        FOR UPDATE SKIP LOCKED
      )
      UPDATE event_log_tenants
      SET next_poll_at = NOW() + interval '${eventLogLeaseTimeoutSeconds} seconds'
      FROM claimed
      WHERE event_log_tenants.tenant_id = claimed.tenant_id
      RETURNING event_log_tenants.tenant_id`,
      [eventLogPrefetchSize]
    )

    const tenants = claimed.rows
    if (tenants.length === 0) {
      return
    }

    await Promise.allSettled(
      tenants.map(async (t) => {
        if (this.signal?.aborted) return

        const lock = await this.semaphore.acquire()
        try {
          await this.processTenant(t.tenant_id)
        } catch (e) {
          logSchema.error(logger, `[EventLogProcessor] Error processing tenant ${t.tenant_id}`, {
            type: 'event-log',
            error: e,
            metadata: JSON.stringify({ tenantId: t.tenant_id }),
          })
        } finally {
          await lock.release()
        }
      })
    )
  }

  private async sweepColdTenants(): Promise<void> {
    try {
      // Use cursor_id (auto-increment) for stable pagination
      const tenants = await this.knex('tenants')
        .select('id', 'cursor_id')
        .where('cursor_id', '>', this.coldCursor)
        .orderBy('cursor_id', 'asc')
        .limit(eventLogSweepBatchSize)

      if (tenants.length === 0) {
        // Reached end of tenants table, reset cursor
        this.coldCursor = 0
        return
      }

      this.coldCursor = tenants[tenants.length - 1].cursor_id

      // Concurrently check which tenants have pending events (semaphore-bounded)
      const tenantsWithEvents: string[] = []

      await Promise.allSettled(
        tenants.map(async (tenant) => {
          if (this.signal?.aborted) return

          const lock = await this.semaphore.acquire()
          try {
            const hasEvents = await this.checkTenantHasEvents(tenant.id)
            if (hasEvents) {
              tenantsWithEvents.push(tenant.id)
            }
          } catch (e) {
            logSchema.warning(
              logger,
              `[EventLogProcessor] Cold sweep error for tenant ${tenant.id}`,
              {
                type: 'event-log',
                error: e,
              }
            )
          } finally {
            await lock.release()
          }
        })
      )

      // Batch insert all discovered tenants at once
      if (tenantsWithEvents.length > 0) {
        const placeholders = tenantsWithEvents.map(() => '(?)').join(',')
        await this.knex.raw(
          `INSERT INTO event_log_tenants (tenant_id) VALUES ${placeholders} ON CONFLICT (tenant_id) DO NOTHING`,
          tenantsWithEvents
        )
      }
    } catch (e) {
      logSchema.error(logger, '[EventLogProcessor] Cold sweep query error', {
        type: 'event-log',
        error: e,
      })
    }
  }

  private async checkTenantHasEvents(tenantId: string): Promise<boolean> {
    try {
      const connection = await this.getTenantConnection(tenantId)
      const result = await connection.raw(
        "SELECT EXISTS(SELECT 1 FROM storage.event_log WHERE status = 'PENDING' LIMIT 1) as has_events"
      )
      return result.rows?.[0]?.has_events ?? false
    } catch {
      return false
    }
  }

  private async processTenant(tenantId: string): Promise<void> {
    const connection = await this.getTenantConnection(tenantId)

    const events = await connection<EventLogRow>('event_log')
      .withSchema('storage')
      .where('status', 'PENDING')
      .orderBy('id', 'asc')
      .limit(eventLogBatchSize)

    if (events.length === 0) {
      // No events remaining - remove from event_log_tenants
      if (isMultitenant) {
        await this.knex('event_log_tenants').where('tenant_id', tenantId).delete()
      }
      return
    }

    // Verify signatures and build job inserts
    const pgBoss = Queue.getInstance()
    const jobInserts: PgBoss.JobInsert[] = []
    const tamperedIds: string[] = []

    for (const event of events) {
      const valid = verifyEventLogSignature(
        event.event_name,
        event.payload,
        event.send_options,
        event.signature
      )

      if (!valid) {
        logSchema.warning(
          logger,
          `[EventLogProcessor] Tampered event detected, skipping id=${event.id}`,
          {
            type: 'event-log',
            metadata: JSON.stringify({
              tenantId,
              eventId: event.id,
              eventName: event.event_name,
            }),
          }
        )
        tamperedIds.push(event.id)
        continue
      }

      const sendOptions = event.send_options || {}
      jobInserts.push({
        ...sendOptions,
        name: event.event_name,
        data: event.payload,
        deadLetter: event.event_name + '-dead-letter',
      })
    }

    // Mark tampered events so they don't block the queue
    if (tamperedIds.length > 0) {
      await connection('event_log')
        .withSchema('storage')
        .whereIn('id', tamperedIds)
        .update({ status: 'SIGNATURE_INVALID' })
    }

    if (jobInserts.length === 0) {
      // All events were tampered, nothing to forward
      return
    }

    try {
      await pgBoss.insert(jobInserts)
    } catch (e) {
      logSchema.error(logger, `[EventLogProcessor] Failed to forward batch for ${tenantId}`, {
        type: 'event-log',
        error: e,
        metadata: JSON.stringify({ tenantId, count: jobInserts.length }),
      })
      // Entire batch failed, will retry on next poll (lease will expire)
      return
    }

    // Delete all successfully forwarded events (tampered ones already marked above)
    const processedIds = events.filter((e) => !tamperedIds.includes(e.id)).map((e) => e.id)
    await connection('event_log').withSchema('storage').whereIn('id', processedIds).delete()

    // Update scheduling in event_log_tenants
    if (isMultitenant) {
      if (events.length >= eventLogBatchSize) {
        // Full batch = more events likely pending. Schedule immediate re-poll.
        await this.knex('event_log_tenants')
          .where('tenant_id', tenantId)
          .update({
            last_polled_at: this.knex.fn.now(),
            next_poll_at: this.knex.fn.now(),
            poll_count: this.knex.raw('poll_count + 1'),
          })
      } else {
        // Partial batch = all events processed. Schedule warm re-poll.
        await this.knex('event_log_tenants')
          .where('tenant_id', tenantId)
          .update({
            last_polled_at: this.knex.fn.now(),
            next_poll_at: this.knex.raw(
              `NOW() + interval '${eventLogWarmPollDelaySeconds} seconds'`
            ),
            poll_count: this.knex.raw('poll_count + 1'),
          })
      }
    }
  }

  private async getTenantConnection(tenantId: string): Promise<Knex> {
    const adminUser = await getServiceKeyUser(tenantId)
    const connection = await getPostgresConnection({
      user: adminUser,
      superUser: adminUser,
      host: tenantId,
      tenantId: tenantId,
      disableHostCheck: true,
    })

    return connection.pool.acquire()
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      if (this.signal?.aborted) {
        resolve()
        return
      }

      const timer = setTimeout(resolve, ms)

      this.signal?.addEventListener(
        'abort',
        () => {
          clearTimeout(timer)
          resolve()
        },
        { once: true }
      )
    })
  }
}
