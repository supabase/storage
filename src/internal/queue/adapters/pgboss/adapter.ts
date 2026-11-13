import { PgBoss } from 'pg-boss'
import type { QueueAdapter } from '../../manager/adapter'
import { json } from '../../manager/codec'
import { createSubscription } from './consumer'
import type { PgBossOptions } from './options'
import type { PgBossCtx } from './pg'
import { sender } from './producer'
import { provisioner } from './queues'
import { statsReport } from './stats'

/** The pg-boss adapter: every optional capability of the contract is present. */
export type PgBossAdapter<W = unknown> = QueueAdapter<W, PgBossCtx> &
  Required<Pick<QueueAdapter<W, PgBossCtx>, 'start' | 'delayedDelivery' | 'stats' | 'maintain'>>

/**
 * A queue adapter over pg-boss v12: one pg-boss queue per queue name, competing consumers on
 * pg-boss's native fetch, transactional produce, native retry budget and dead-lettering.
 * Jobs sent with plain `boss.send()` to the same queue are delivered too (bare payload, empty
 * headers), and plain pg-boss workers can consume what this adapter sends.
 */
export function pgboss<W = unknown>(options: PgBossOptions<W>): PgBossAdapter<W> {
  if ((options.boss === undefined) === (options.connectionString === undefined)) {
    throw new Error(
      'queue pgboss: pass exactly one of "boss" (caller-owned) or "connectionString" (adapter-owned)'
    )
  }
  const schema = options.schema ?? options.bossOptions?.schema ?? 'pgboss'
  if (!/^[A-Za-z0-9_]+$/.test(schema)) {
    throw new Error(`queue pgboss: invalid schema "${schema}"`)
  }

  // Supervision follows ownership (see PgBossOptions).
  const owned = options.boss === undefined
  const boss =
    options.boss ??
    new PgBoss({
      ...options.bossOptions,
      connectionString: options.connectionString,
      supervise: true,
      schedule: false,
    })
  if (owned) {
    // An unlistened 'error' event would take the process down.
    boss.on('error', (error) => options.events?.maintenanceError?.({ job: 'supervise', error }))
  }

  const queues = provisioner(boss, options.queue ?? {})

  return {
    name: 'pgboss',
    codec: options.codec ?? json<W>(),
    delayedDelivery: true,

    // Idempotent. The owned path migrates here; a caller-owned instance follows its own flags.
    start: () => boss.start().then(() => undefined),

    async close() {
      if (owned) await boss.stop({ graceful: false, close: true })
    },

    provision: queues.provision,
    send: sender<W>(boss),

    // The queue's policy is written by `provision`, never here. A queue nobody provisioned is
    // created on first subscribe, from its declared options when the registry has them.
    async subscribe(queue, policy) {
      await queues.ensure(queue, policy.queues?.get(queue)?.options)
      return createSubscription<W>(boss, schema, queue)
    },

    stats: (query, signal) => statsReport(boss, schema, query, signal),

    // One supervision pass: expiry/heartbeat sweeps, retention, queue stats. DB-gated, so a
    // pass that is not due costs one conditional UPDATE. On a caller-owned instance with
    // supervision off, THIS is what sweeps.
    maintain: () => boss.supervise(),
  }
}
