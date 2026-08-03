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
import type {
  AnyMessageClass,
  ProduceCall,
  ProduceMessage,
  WaveMiddleware,
} from '@supabase-labs/wave-core'
import { MESSAGE_TYPE_HEADER } from '@supabase-labs/wave-core'
import { getConfig } from '../../config'
import { SYSTEM_TENANT_REF } from './constants'
import { isBasePayload, type QueueEventOptions } from './events'

type AnyWaveMiddleware = WaveMiddleware

const typeOf = (m: ProduceMessage<unknown>): string | undefined => m.headers?.[MESSAGE_TYPE_HEADER]

/** Whether a bound message class carries the storage queue-middleware statics — declared by
 * `storageEvent` (events/base.ts), not part of wave's `AnyMessageClass` contract. A runtime
 * check, so a class from outside `storageEvent` simply gets no gating config rather than
 * being blindly trusted to have it. */
const isQueueEventClass = (cls: AnyMessageClass): cls is AnyMessageClass & QueueEventOptions =>
  typeof (cls as Partial<QueueEventOptions>).eventType === 'string'

/**
 * Resolve the message class actually bound to this produce call's topic, via `call.getTopic` —
 * no registry threaded in as a constructor argument. `allowSync`/`disableKeys`/`eventType` are
 * read directly off the class (see `storageEvent` in events/base.ts): the class IS the
 * single source of truth, not a parallel metadata object. Polymorphic topics (more than one
 * bound class) disambiguate by the wire `message-type` header; the single-class case (every
 * storage topic today) needs no disambiguation.
 */
const classOf = (
  call: ProduceCall,
  messageType: string | undefined
): QueueEventOptions | undefined => {
  const classes = call.getTopic(call.topic)?.classes?.filter(isQueueEventClass)

  if (classes === undefined || classes.length === 0) return undefined
  if (classes.length === 1) return classes[0]
  return classes.find((cls) => cls.eventType === messageType) ?? classes[0]
}

const tenantRefOf = (m: ProduceMessage<unknown>): string =>
  (isBasePayload(m.data) ? m.data.tenant.ref : undefined) || SYSTEM_TENANT_REF

/**
 * Per-tenant event disabling (v1 `shouldSend`): in multitenant mode, a message whose
 * disable keys intersect the tenant's `disableEvents` is silently dropped — the produce call
 * still resolves, exactly as v1's gated `send()` resolved to nothing. Applies on the real
 * wave AND the sync wave (v1 gates before the env-disabled branch). A message whose tenant
 * no longer exists is dropped the same way rather than failing the produce.
 */
export function tenantDisableEvents(): AnyWaveMiddleware {
  const { isMultitenant } = getConfig()
  return {
    produce: (next) => async (call) => {
      if (!isMultitenant) return next(call)

      const kept: ProduceMessage<unknown>[] = []
      for (const m of call.messages) {
        const cls = classOf(call, typeOf(m))
        // System-produced messages have no tenant config row to consult — never gated.
        if (
          !isBasePayload(m.data) ||
          cls === undefined ||
          m.data.tenant.ref === SYSTEM_TENANT_REF
        ) {
          kept.push(m)
          continue
        }
        let disabled: string[]
        try {
          disabled = (await getTenantConfig(m.data.tenant.ref)).disableEvents || []
        } catch (e) {
          // A deleted tenant's messages drop like a disabled event (the produce still
          // resolves); any other lookup failure fails the produce rather than guessing.
          if (e instanceof StorageBackendError && e.code === ErrorCode.TenantNotFound) continue
          throw e
        }
        const keys = cls.disableKeys?.(m.data) ?? [cls.eventType]
        if (!keys.some((key) => disabled.includes(key))) kept.push(m)
      }

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
 * `queue_job_retry_failed` per handler failure (v1 counted every failed handle, terminal
 * or not), and `queue_job_run_time_seconds` around the invocation — one handler call on the
 * single-message workers storage runs, one `handleBatch` slice on a batch worker — labeled
 * `status` with its outcome (`ok`/`error`/`timeout`/`shutdown`/`fenced`). Released messages
 * (timeout/shutdown/fence) consume no retry budget and count toward neither counter, as in
 * v1. The other two v1 consume metrics have no middleware seam:
 * `queue_job_error` (a message exhausted its retry budget) is the ADAPTER's verdict — an
 * envelope carries only its `attempt`, never the budget — so it rides the pgque adapter's
 * `deadLettered` event (wired in instance.ts; pg-boss's engine verdicts invisibly and cannot
 * emit it), and `queue_job_complete_failed` has none at all: settlement is adapter-owned.
 *
 * Every series carries the engine behind it (`adapter="pgque"|"pgboss"`), so a fleet mixing
 * engines — or a dashboard comparing them — can tell the two apart.
 */
export function schedulingMetrics(): AnyWaveMiddleware {
  const { pgQueueAdapter: adapter } = getConfig()
  return {
    produce: (next) => async (call) => {
      const startTime = performance.now()
      try {
        await next(call)
        queueJobScheduled.add(call.messages.length, { name: call.topic, adapter })
      } finally {
        const duration = (performance.now() - startTime) / 1000
        queueJobSchedulingTime.record(duration, { name: call.topic, adapter })
      }
    },
    consume: (next) => async (call) => {
      const startTime = performance.now()
      const result = await next(call)
      const duration = (performance.now() - startTime) / 1000
      queueJobRunTime.record(duration, { name: call.topic, adapter, status: result.outcome })
      if (result.ok > 0) {
        queueJobCompleted.add(result.ok, { name: call.topic, adapter })
      }
      if (result.failures.length > 0) {
        queueJobRetryFailed.add(result.failures.length, { name: call.topic, adapter })
      }
      return result
    },
  }
}

/**
 * v1's fault-tolerance seam: a failed enqueue (timeout included) degrades to running the
 * handler synchronously via the sync wave — which dispatches on producer-only nodes too and
 * tolerates nested sends. Faithful to v1 in both directions: only single-message calls fall
 * back (v1's `batchSend` had no fallback — batch errors propagate raw), and `allowSync: false`
 * events rethrow instead. A fallen-back handler may ALSO run later from the queue (the append
 * outcome is unknown on timeout) and must tolerate duplicates — same contract as v1.
 */
export function syncFallback(): AnyWaveMiddleware {
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

        await call.invoke(call.topic, single.data, { key: single.key, headers: single.headers })
      }
    },
  }
}

/**
 * The env-disabled posture for events that must never run in-process (v1 `allowSync: false`
 * under `!pgQueueEnable`): warn and skip, never throw. Composed into the SYNC wave only —
 * on the real wave the same events rethrow from the fallback instead.
 */
export function syncModeGuard(): AnyWaveMiddleware {
  return {
    produce: (next) => async (call) => {
      const kept = call.messages.filter((m) => {
        const cls = classOf(call, typeOf(m))
        if (cls === undefined || (cls.allowSync ?? true)) return true
        logger.warn({ type: 'queue', eventType: cls.eventType }, '[Queue] skipped sending message')
        return false
      })
      if (kept.length === 0) return
      await next(kept.length === call.messages.length ? call : { ...call, messages: kept })
    },
  }
}
