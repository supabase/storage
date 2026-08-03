import { getTenantConfig } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { queueJobScheduled, queueJobSchedulingTime } from '@internal/monitoring/metrics'
import type { AnyMessageClass, ProduceCall, ProduceMessage, WaveMiddleware } from '@supabase-labs/wave-core'
import { MESSAGE_TYPE_HEADER } from '@supabase-labs/wave-core'
import { getConfig } from '../../config'
import { SYSTEM_TENANT_REF } from './constants'
import { isBasePayload, type QueueEventOptions } from './events'

type AnyWaveMiddleware = WaveMiddleware<unknown>

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
 * wave AND the sync wave (v1 gates before the env-disabled branch).
 */
export function tenantDisableEvents(): AnyWaveMiddleware {
  const { isMultitenant } = getConfig()
  return {
    produce: (next) => async (call) => {
      if (!isMultitenant) return next(call)

      const kept: ProduceMessage<unknown>[] = []
      for (const m of call.messages) {
        const cls = classOf(call, typeOf(m))
        if (!isBasePayload(m.data) || cls === undefined) {
          kept.push(m)
          continue
        }
        const disabled = (await getTenantConfig(m.data.tenant.ref)).disableEvents || []
        const keys = cls.disableKeys?.(m.data) ?? [cls.eventType]
        if (!keys.some((key) => disabled.includes(key))) kept.push(m)
      }

      if (kept.length === 0) return
      await next(kept.length === call.messages.length ? call : { ...call, messages: kept })
    },
  }
}

/** v1's scheduling metrics, verbatim names: `queue_job_scheduled` per landed message and
 * `queue_job_scheduled_time_seconds` around the whole produce (fallback included, as v1's
 * `finally` measured). */
export function schedulingMetrics(): AnyWaveMiddleware {
  return {
    produce: (next) => async (call) => {
      const startTime = performance.now()
      try {
        await next(call)
        queueJobScheduled.add(call.messages.length, { name: call.topic })
      } finally {
        const duration = (performance.now() - startTime) / 1000
        queueJobSchedulingTime.record(duration, { name: call.topic })
      }
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

        await call.invoke!(call.topic, single.data, { key: single.key, headers: single.headers })
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
