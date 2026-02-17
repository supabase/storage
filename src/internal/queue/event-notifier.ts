import { Knex } from 'knex'
import { getConfig } from '../../config'
import { logger, logSchema } from '@internal/monitoring'

const { isMultitenant } = getConfig()

let instance: EventNotifier | undefined

export class EventNotifier {
  private pending = new Set<string>()
  private flushInterval: ReturnType<typeof setInterval> | undefined

  constructor(private readonly knex: Knex) {}

  static init(multitenantKnex: Knex): EventNotifier {
    instance = new EventNotifier(multitenantKnex)
    instance.start()
    return instance
  }

  static getInstance(): EventNotifier | undefined {
    return instance
  }

  static destroy() {
    instance?.stop()
    instance = undefined
  }

  start() {
    this.flushInterval = setInterval(() => {
      this.flush().catch((e) => {
        logSchema.warning(logger, '[EventLogNotifier] Flush error', {
          type: 'event-log',
          error: e,
        })
      })
    }, 1000)
  }

  stop() {
    if (this.flushInterval) {
      clearInterval(this.flushInterval)
      this.flushInterval = undefined
    }
    this.pending.clear()
  }

  /**
   * Enqueue a tenant for notification.
   * Deduped by the Set within each flush window.
   * DB deduped by ON CONFLICT DO NOTHING across windows.
   */
  notify(tenantId: string): void {
    if (!isMultitenant) {
      return
    }

    this.pending.add(tenantId)
  }

  /**
   * Flush pending tenant notifications in chunked batch inserts.
   * Swaps the Set reference to avoid Array.from() and clear() allocations.
   * Reuses a single batch array to minimise GC pressure.
   */
  private async flush(): Promise<void> {
    if (this.pending.size === 0) {
      return
    }

    // Swap: new calls accumulate into a fresh Set while we flush the old one
    const toFlush = this.pending
    this.pending = new Set()

    const chunkSize = 500
    let batch: string[] = []

    for (const tenantId of toFlush) {
      batch.push(tenantId)

      if (batch.length >= chunkSize) {
        await this.insertBatch(batch)
        batch = []
      }
    }

    if (batch.length > 0) {
      await this.insertBatch(batch)
    }
  }

  private async insertBatch(chunk: string[]): Promise<void> {
    try {
      const placeholders = chunk.map(() => '(?)').join(',')
      await this.knex.raw(
        `INSERT INTO event_log_tenants (tenant_id) VALUES ${placeholders} ON CONFLICT (tenant_id) DO NOTHING`,
        chunk
      )
    } catch (e) {
      logSchema.warning(logger, '[EventLogNotifier] Failed to notify tenants (best-effort)', {
        type: 'event-log',
        error: e,
        metadata: JSON.stringify({ count: chunk.length }),
      })
    }
  }
}
