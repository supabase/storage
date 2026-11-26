import { getTenantConfig } from '@internal/database'
import { ErrorCode, StorageBackendError } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import {
  queueJobCompleted,
  queueJobRetryFailed,
  queueJobRunTime,
  queueJobScheduled,
  queueJobSchedulingTime,
  queueJobSyncInvocations,
  queueJobWaitTime,
} from '@internal/monitoring/metrics'
import { getConfig } from '../../config'
import { SYSTEM_TENANT_REF } from './constants'
import { isBasePayload, type QueueEventOptions } from './events'
import type { AnyMessageClass, ProduceCall, ProduceMessage, QueueMiddleware } from './manager'
import {
  DELIVER_AFTER_HEADER,
  InlineCascadeError,
  MESSAGE_TYPE_HEADER,
  PartialSendError,
} from './manager'

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
 * Why a handler ran in-process, on the caller, instead of from the queue:
 *
 * - `invoke`: the caller asked for it (`queue.invoke`, e.g. `BucketDeleted.invoke`).
 * - `queue_disabled`: the queue is env-disabled (`PG_QUEUE_ENABLE=false`), so every produce
 *   runs its handler inline.
 * - `send_failed`: the queue was enabled but the send failed, and {@link syncFallback} ran the
 *   handler instead.
 */
export type SyncInvocationReason = 'invoke' | 'queue_disabled' | 'send_failed'

/**
 * Run `dispatch`, which runs the handler of `messages` in-process, and record it: the
 * `queue_job_sync_invocations_total` counter (one per message, labeled with the queue, the
 * reason and `status="ok"|"error"`) and one `[Queue] Invoked synchronously` log line per call.
 * A failed dispatch still rejects with its own error. A refused inline cascade
 * (`InlineCascadeError`) ran nothing and is not recorded. A batch can span tenants, so its log
 * line names a project only when every message belongs to the same one.
 */
export async function observeSyncInvocation(
  queue: string,
  reason: SyncInvocationReason,
  messages: readonly ProduceMessage<unknown>[],
  dispatch: () => Promise<void>
): Promise<void> {
  const startTime = performance.now()
  try {
    await dispatch()
  } catch (error) {
    if (!(error instanceof InlineCascadeError)) {
      recordSyncInvocation(queue, reason, messages, startTime, { error })
    }
    throw error
  }
  recordSyncInvocation(queue, reason, messages, startTime)
}

function recordSyncInvocation(
  queue: string,
  reason: SyncInvocationReason,
  messages: readonly ProduceMessage<unknown>[],
  startTime: number,
  failure?: { error: unknown }
): void {
  const status = failure === undefined ? 'ok' : 'error'
  queueJobSyncInvocations.add(messages.length, { name: queue, reason, status })

  // A log line must never change how the dispatch settles.
  try {
    const [first] = messages
    const tenants = new Set(messages.map(tenantRefOf))
    const [project] = tenants.size === 1 ? tenants : [undefined]
    const log = {
      type: 'queue',
      project,
      sbReqId: first !== undefined && isBasePayload(first.data) ? first.data.sbReqId : undefined,
      metadata: JSON.stringify({
        queue,
        reason,
        status,
        count: messages.length,
        eventTypes: [...new Set(messages.map(typeOf).filter((t) => t !== undefined))],
        durationMs: Math.round(performance.now() - startTime),
      }),
    }
    if (failure === undefined) {
      logSchema.info(logger, '[Queue] Invoked synchronously', log)
    } else {
      logSchema.warning(logger, '[Queue] Invoked synchronously', { ...log, error: failure.error })
    }
  } catch {
    // ignore
  }
}

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
 * Consume, before the handler runs: `queue_job_wait_time_seconds` per message, the time from
 * when the message became deliverable to when its handler call starts. That is when the queue
 * backend took it, or, for a message sent with a delay, when the delay ran out: the delay is
 * asked for, not time the queue made the message wait. The time a pulled message spends in a
 * busy worker's window does count: the message was ready and nothing was running it. Only a
 * message's first delivery is recorded: a redelivery waited out its retry backoff on purpose.
 * Messages run in-process are skipped too: they never queued. The series carry the queue and
 * the engine and nothing per tenant or per message.
 *
 * Consume, from each invocation's `BatchResult`: `queue_job_completed` per handled message,
 * `queue_job_retry_failed` per handler failure (v1 counted every failed handle, terminal or
 * not), and `queue_job_run_time_seconds` around the invocation, labeled `status` with its
 * outcome (`ok`/`error`/`timeout`/`shutdown`). Messages cut off by a timeout or a
 * shutdown count toward neither counter: the `status` label is where they show. A timeout
 * still uses a retry attempt; a shutdown does not. `queue_job_error` (a message exhausted
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
      const pickedUpAt = Date.now()
      for (const message of call.messages) {
        if (message.attempt !== 1 || isSyncDispatch(message)) continue
        // A delayed message is only deliverable once its delay runs out (NaN when it had none).
        const deliverAfter = Number(message.headers[DELIVER_AFTER_HEADER])
        const deliverableAt = deliverAfter > message.timestamp ? deliverAfter : message.timestamp
        // The times come from other clocks than this one; a little skew must not go negative.
        const waited = Math.max(0, pickedUpAt - deliverableAt) / 1000
        queueJobWaitTime.record(waited, { name: call.queue, adapter })
      }

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

/** `<tenant>/<bucket>/<object>` when the payload names them, as far as it does. A webhook
 * carries them one level down, on the event it wraps; a CDN purge under `purgeOptions`. */
function objectPathOf(data: unknown): string {
  const tenant = isBasePayload(data) ? data.tenant.ref : SYSTEM_TENANT_REF
  const fields = data as {
    event?: { payload?: unknown }
    purgeOptions?: { bucket?: unknown; objectName?: unknown }
  } | null
  const source = (fields?.event?.payload ?? data) as { bucketId?: unknown; name?: unknown } | null
  const bucket = source?.bucketId ?? fields?.purgeOptions?.bucket
  const object = source?.name ?? fields?.purgeOptions?.objectName
  const parts = [tenant]
  if (typeof bucket === 'string') {
    parts.push(bucket)
    if (typeof object === 'string') parts.push(object)
  }
  return parts.join('/')
}

/**
 * One log line per message a handler settled, on every queue: `[<Message type>]: <path> -
 * COMPLETED` or `- FAILED`, with the payload, the tenant and request ids it carries, the
 * delivery attempt and how long the handler call took. The label is the message's type (its
 * class's declared `type`), or the queue name for a message sent without a class.
 *
 * `durationMs` is the handler call the message was part of: its own run for a single-message
 * handler, the whole batch for a batch handler.
 *
 * A handler call that ran out of its `consumeTimeout` is logged as a warning, `- TIMED OUT`,
 * for each of its messages: nothing settled, but the delivery used an attempt, and a handler
 * that always times out would otherwise show in no log until it dead-letters. Messages
 * released by a shutdown are not logged.
 */
export function completionLog(): QueueMiddleware {
  return {
    consume: (next) => async (call) => {
      const startTime = performance.now()
      const result = await next(call)
      if (result.outcome === 'shutdown') return result
      const timedOut = result.outcome === 'timeout'
      const durationMs = Math.round(performance.now() - startTime)
      const errors = new Map(result.failures.map((failure) => [failure.message, failure.error]))
      // Failed messages are usually still in the call; ones a stage rejected are not.
      for (const message of new Set([...call.messages, ...errors.keys()])) {
        // A log line must never change how a message settles.
        try {
          const { data } = message
          const label = message.type ?? call.queue
          const path = objectPathOf(data)
          const base = isBasePayload(data) ? data : undefined
          const wrappedType = (data as { event?: { type?: unknown } } | null)?.event?.type
          const detail = typeof wrappedType === 'string' ? `${wrappedType} ` : ''
          const log = {
            jobId: message.id,
            type: 'event' as const,
            event: label,
            payload: JSON.stringify(data),
            objectPath: path,
            resources: [`/${path}`],
            tenantId: base?.tenant.ref,
            project: base?.tenant.ref,
            reqId: base?.reqId,
            sbReqId: base?.sbReqId,
            attempt: message.attempt,
            durationMs,
          }
          if (timedOut) {
            logger.warn(log, `[${label}]: ${detail}${path} - TIMED OUT`)
          } else if (errors.has(message)) {
            const error = errors.get(message)
            logger.error({ ...log, error }, `[${label}]: ${detail}${path} - FAILED`)
          } else {
            logSchema.event(logger, `[${label}]: ${detail}${path} - COMPLETED`, log)
          }
        } catch {
          // ignore
        }
      }
      return result
    },
  }
}

/**
 * v1's fault-tolerance seam: a failed enqueue (timeout included) degrades to running the
 * handler synchronously, which dispatches on producer-only nodes too and tolerates nested
 * sends. `allowSync: false` events rethrow instead. A fallen-back handler may ALSO run later
 * from the queue (the send outcome is unknown on timeout) and must tolerate duplicates — same
 * contract as v1.
 *
 * A batch falls back the same way, run as the handler's worker would run it: a batch handler
 * gets slices no larger than its `prefetch`, and `parallelism` invocations run at a time.
 *
 * - Only what the queue did not take is run. A send that names the messages it could not
 *   confirm (`PartialSendError`) falls back for those alone; any other failure says nothing
 *   about which messages landed, so all of them run and the ones that did land run twice.
 * - The caller's `signal` bounds the fallback. Once it aborts no further handler starts, and
 *   the call rejects with the send failure: the messages left over were neither queued nor
 *   run.
 * - The call also rejects when a message could not be run in-process (with the send failure)
 *   or its handler failed (with the first such failure), after every other message had its
 *   turn.
 * - A handler run this way may send in turn, and those sends fall back too: a page of deletes
 *   run in-process still gets its webhooks out. What is refused is a handler falling back
 *   into its own queue (the next page of the same delete), which would run a whole backlog
 *   inside one caller: that send rejects with the send failure, and nothing more is run.
 */
export function syncFallback(): QueueMiddleware {
  return {
    produce: (next) => async (call) => {
      try {
        await next(call)
      } catch (e) {
        const [first] = call.messages
        if (first === undefined) throw e
        const isBatch = call.messages.length > 1
        // A single message of no known class is not ours to fall back for.
        if (!isBatch && classOf(call, typeOf(first)) === undefined) throw e

        const unsent =
          e instanceof PartialSendError
            ? call.messages.filter((_, position) => e.unsent.has(position))
            : call.messages
        const [firstUnsent] = unsent
        // Every message was confirmed after all: there is nothing left to run.
        if (firstUnsent === undefined) return

        logSchema.warning(
          logger,
          isBatch
            ? `[Queue Sender] Error while sending ${unsent.length} of ${call.messages.length} jobs to queue, sending synchronously`
            : `[Queue Sender] Error while sending job to queue, sending synchronously`,
          {
            type: 'queue',
            // A batch can span tenants, and what failed is the send: its warning stays ours
            // and is not routed to whichever tenant comes first in it.
            ...(isBatch ? {} : { project: tenantRefOf(firstUnsent) }),
            error: e,
            // One payload stands for the batch: a thousand of them is not a log line.
            metadata: JSON.stringify(firstUnsent.data),
            sbReqId: isBasePayload(firstUnsent.data) ? firstUnsent.data.sbReqId : undefined,
          }
        )

        const runnable = unsent.filter((message) => {
          const cls = classOf(call, typeOf(message))
          return cls !== undefined && (cls.allowSync ?? true)
        })
        const notDispatched = runnable.length < unsent.length
        if (runnable.length > 0) {
          try {
            await observeSyncInvocation(call.queue, 'send_failed', runnable, () =>
              call.invokeMany(
                call.queue,
                runnable.map((message) => ({
                  data: message.data,
                  key: message.key,
                  headers: asSyncDispatch(message).headers,
                })),
                call.signal !== undefined ? { signal: call.signal } : undefined
              )
            )
          } catch (handlerError) {
            // The caller's bound ran out with messages still waiting for their turn.
            const outOfTime = call.signal?.aborted === true && handlerError === call.signal.reason
            // This queue's handler is already running inline up the chain (it re-sent itself):
            // nothing was run, so the failure to report is the send's.
            const refused = handlerError instanceof InlineCascadeError
            if (outOfTime || refused || notDispatched) throw e
            throw handlerError
          }
        }
        if (notDispatched) throw e
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
      const messages = kept.map(asSyncDispatch)
      await observeSyncInvocation(call.queue, 'queue_disabled', messages, () =>
        next({ ...call, messages })
      )
    },
  }
}
