import { getTenantConfig } from '@internal/database'
import { ErrorCode, StorageBackendError } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import {
  queueJobCompleted,
  queueJobRetryFailed,
  queueJobRunTime,
  queueJobScheduled,
  queueJobSchedulingTime,
} from '@internal/monitoring/metrics'
import { getConfig } from '../../config'
import { SYSTEM_TENANT_REF } from './constants'
import { isBasePayload, type QueueEventOptions } from './events'
import type { AnyMessageClass, ProduceCall, ProduceMessage, QueueMiddleware } from './manager'
import { MESSAGE_TYPE_HEADER } from './manager'

/** Tenant config lookups one produce call runs at a time. */
const TENANT_LOOKUP_CONCURRENCY = 20

const typeOf = (m: ProduceMessage<unknown>): string | undefined => m.headers?.[MESSAGE_TYPE_HEADER]

/** Whether a bound message class carries the storage queue-middleware statics, declared by
 * `storageEvent` (events/base.ts). A runtime check, so a class from outside `storageEvent`
 * simply gets no gating config rather than being blindly trusted to have it. */
const isQueueEventClass = (cls: AnyMessageClass): cls is AnyMessageClass & QueueEventOptions =>
  typeof (cls as Partial<QueueEventOptions>).eventType === 'string'

/**
 * Resolve the message class bound to this produce call's queue. `allowSync`/`disableKeys`/
 * `eventType` are read directly off the class (see `storageEvent` in events/base.ts).
 * Polymorphic queues (more than one bound class) disambiguate by the wire `message-type`
 * header.
 */
const classOf = (
  call: ProduceCall,
  messageType: string | undefined
): QueueEventOptions | undefined => {
  const classes = call.getQueue(call.queue)?.classes?.filter(isQueueEventClass)

  if (classes === undefined || classes.length === 0) return undefined
  if (classes.length === 1) return classes[0]
  return classes.find((cls) => cls.eventType === messageType) ?? classes[0]
}

/**
 * Stamped on a message whose handler runs in-process, inside the produce call, instead of
 * from the queue: the env-disabled sync queue and the produce-failure fallback. v1 marked the
 * same two paths with the `__sync` job id.
 */
export const SYNC_DISPATCH_HEADER = 'x-sync-dispatch'

const asSyncDispatch = <T>(m: ProduceMessage<T>): ProduceMessage<T> => ({
  ...m,
  headers: { ...m.headers, [SYNC_DISPATCH_HEADER]: '1' },
})

/**
 * Whether a handler is running in-process on the producer's call stack rather than from the
 * queue. Nothing redelivers such a run, and a throw lands in the producer's request — so a
 * handler whose failure is best-effort there (but must retry on the queue) branches on this.
 */
export function isSyncDispatch(message: {
  readonly headers?: Readonly<Record<string, string>>
}): boolean {
  return message.headers?.[SYNC_DISPATCH_HEADER] === '1'
}

const tenantRefOf = (m: ProduceMessage<unknown>): string =>
  (isBasePayload(m.data) ? m.data.tenant.ref : undefined) || SYSTEM_TENANT_REF

/**
 * Per-tenant event disabling (v1 `shouldSend`): in multitenant mode, a message whose
 * disable keys intersect the tenant's `disableEvents` is silently dropped — the produce call
 * still resolves, exactly as v1's gated `send()` resolved to nothing. Applies on the real
 * queue AND the sync queue (v1 gates before the env-disabled branch). A message whose tenant
 * no longer exists is dropped the same way rather than failing the produce.
 */
export function tenantDisableEvents(): QueueMiddleware {
  const { isMultitenant } = getConfig()
  return {
    produce: (next) => async (call) => {
      if (!isMultitenant) return next(call)

      // The disable keys of each message that can be gated at all. System-produced messages
      // have no tenant config row to consult. A class declaring no keys is never gated and
      // costs no lookup: it must still be sent for a tenant whose row is already gone
      // (cleanup after a tenant delete), and fleet-wide fan-outs must not load every tenant.
      const gated = new Map<ProduceMessage<unknown>, { ref: string; keys: string[] }>()
      for (const m of call.messages) {
        const cls = classOf(call, typeOf(m))
        if (!isBasePayload(m.data) || cls === undefined) continue
        const ref = m.data.tenant.ref
        if (ref === SYSTEM_TENANT_REF) continue
        const keys = cls.disableKeys?.(m.data) ?? [cls.eventType]
        if (keys.length > 0) gated.set(m, { ref, keys })
      }
      if (gated.size === 0) return next(call)

      // One lookup per distinct tenant, a bounded number at a time.
      const refs = [...new Set([...gated.values()].map((g) => g.ref))]
      const disabledBy = new Map<string, string[] | undefined>()
      for (let i = 0; i < refs.length; i += TENANT_LOOKUP_CONCURRENCY) {
        await Promise.all(
          refs.slice(i, i + TENANT_LOOKUP_CONCURRENCY).map(async (ref) => {
            try {
              disabledBy.set(ref, (await getTenantConfig(ref)).disableEvents || [])
            } catch (e) {
              // A deleted tenant's messages drop like a disabled event (the produce still
              // resolves); any other lookup failure fails the produce rather than guessing.
              if (e instanceof StorageBackendError && e.code === ErrorCode.TenantNotFound) {
                disabledBy.set(ref, undefined)
                return
              }
              throw e
            }
          })
        )
      }

      const kept = call.messages.filter((m) => {
        const gate = gated.get(m)
        if (gate === undefined) return true
        const disabled = disabledBy.get(gate.ref)
        return disabled !== undefined && !gate.keys.some((key) => disabled.includes(key))
      })

      if (kept.length === 0) return
      await next(kept.length === call.messages.length ? call : { ...call, messages: kept })
    },
  }
}

/** v1's queue metrics, verbatim names, both directions.
 *
 * Produce: `queue_job_scheduled` per landed message and `queue_job_scheduled_time_seconds`
 * around the whole produce (fallback included, as v1's `finally` measured).
 *
 * Consume, from each invocation's `BatchResult`: `queue_job_completed` per handled message,
 * `queue_job_retry_failed` per handler failure (v1 counted every failed handle, terminal or
 * not), and `queue_job_run_time_seconds` around the invocation, labeled `status` with its
 * outcome (`ok`/`error`/`timeout`/`shutdown`). Released messages (timeout/shutdown) consume
 * no retry budget and count toward neither counter. `queue_job_error` (a message exhausted
 * its retry budget) is the adapter's verdict and rides its `deadLettered` event where the
 * engine lets it observe one (wired in instance.ts; pg-boss verdicts natively and cannot).
 *
 * Every series carries the engine behind it (`adapter="pgboss"|"sqs"`).
 */
export function schedulingMetrics(): QueueMiddleware {
  const { queueAdapter: adapter } = getConfig()
  return {
    produce: (next) => async (call) => {
      const startTime = performance.now()
      try {
        await next(call)
        queueJobScheduled.add(call.messages.length, { name: call.queue, adapter })
      } finally {
        const duration = (performance.now() - startTime) / 1000
        queueJobSchedulingTime.record(duration, { name: call.queue, adapter })
      }
    },
    consume: (next) => async (call) => {
      const startTime = performance.now()
      const result = await next(call)
      const duration = (performance.now() - startTime) / 1000
      queueJobRunTime.record(duration, { name: call.queue, adapter, status: result.outcome })
      if (result.ok > 0) {
        queueJobCompleted.add(result.ok, { name: call.queue, adapter })
      }
      if (result.failures.length > 0) {
        queueJobRetryFailed.add(result.failures.length, { name: call.queue, adapter })
      }
      return result
    },
  }
}

/**
 * v1's fault-tolerance seam: a failed enqueue (timeout included) degrades to running the
 * handler synchronously via `invoke`, which dispatches on producer-only nodes too and
 * tolerates nested sends. Faithful to v1 in both directions: only single-message calls fall
 * back (v1's `batchSend` had no fallback — batch errors propagate raw), and `allowSync: false`
 * events rethrow instead. A fallen-back handler may ALSO run later from the queue (the send
 * outcome is unknown on timeout) and must tolerate duplicates — same contract as v1.
 */
export function syncFallback(): QueueMiddleware {
  return {
    produce: (next) => async (call) => {
      try {
        await next(call)
      } catch (e) {
        const single = call.messages.length === 1 ? call.messages[0] : undefined
        const cls = single !== undefined ? classOf(call, typeOf(single)) : undefined
        if (single === undefined || cls === undefined) throw e

        logSchema.warning(
          logger,
          `[Queue Sender] Error while sending job to queue, sending synchronously`,
          {
            type: 'queue',
            project: tenantRefOf(single),
            error: e,
            metadata: JSON.stringify(single.data),
            sbReqId: isBasePayload(single.data) ? single.data.sbReqId : undefined,
          }
        )

        if (!(cls.allowSync ?? true)) throw e

        await call.invoke(call.queue, single.data, {
          key: single.key,
          headers: asSyncDispatch(single).headers,
        })
      }
    },
  }
}

/**
 * The env-disabled posture for events that must never run in-process (v1 `allowSync: false`
 * under `!pgQueueEnable`): warn and skip, never throw. Composed into the SYNC queue only;
 * on the real queue the same events rethrow from the fallback instead.
 */
export function syncModeGuard(): QueueMiddleware {
  return {
    produce: (next) => async (call) => {
      const kept = call.messages.filter((m) => {
        const cls = classOf(call, typeOf(m))
        if (cls === undefined || (cls.allowSync ?? true)) return true
        logger.warn({ type: 'queue', eventType: cls.eventType }, '[Queue] skipped sending message')
        return false
      })
      if (kept.length === 0) return
      // Everything this queue produces runs inline: mark it so handlers can tell.
      await next({ ...call, messages: kept.map(asSyncDispatch) })
    },
  }
}
