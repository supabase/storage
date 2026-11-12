import { NotSupportedError, type ProducerAdapter, type SendContext } from './adapter'
import type { Codec } from './codec'
import { isMessageInstance, type MessageInstance, pickMessage, toProduceMessage } from './declare'
import type { QueueEvents } from './events'
import type { InvokeApi } from './invoke'
import type { ProduceMessage } from './message'
import {
  type ProduceCall,
  type ProduceNext,
  produceStages,
  type QueueMiddleware,
} from './middleware'
import { type QueuesView, resolveMessageQueue } from './registry'

/** Options for the single-message `produce(queue, data, options)` form: everything a
 * {@link ProduceMessage} carries besides its data, plus the per-call context and bound. */
export interface ProduceOptions<Ctx = never> extends Omit<ProduceMessage<unknown>, 'data'> {
  /** Adapter-specific produce context, shaped by the adapter's declared `Ctx` (typically the
   * caller's transaction). Adapters without one leave `Ctx = never`, so passing it is a
   * compile error there. */
  readonly ctx?: Ctx
  /**
   * Bound this produce call: when the signal aborts, the call rejects with `signal.reason`.
   * Composed with the manager's `produceTimeout`. The bound is on the WAIT, not the send: an
   * abandoned send may still land, so anything acting on the failure must tolerate the
   * message turning up later.
   */
  readonly signal?: AbortSignal
}

/** Options for the batch and message-instance forms; per-message fields live on each message. */
export interface ProduceBatchOptions<Ctx = never> {
  readonly ctx?: Ctx
  readonly signal?: AbortSignal
}

export interface ProducerApi {
  /** Single form (`produce(queue, data, options?)`), batch form (`produce(queue, [{ data,
   * … }], options?)`) or message-instance form (`produce(instance | instances, options?)`),
   * disambiguated by shape. */
  produce(
    queueOrMessages: string | MessageInstance | ReadonlyArray<MessageInstance>,
    dataOrMessagesOrOptions?: unknown,
    options?: ProduceOptions<unknown>
  ): Promise<void>
}

// Race an in-flight send against its bound. On abort the caller stops waiting while the
// abandoned send keeps running and may still land; its eventual settle is absorbed here, so
// a late failure never surfaces as an unhandled rejection.
const raceAbort = (pending: Promise<void>, signal: AbortSignal): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    const onAbort = (): void => reject(signal.reason)
    signal.addEventListener('abort', onAbort, { once: true })
    pending.then(
      () => {
        signal.removeEventListener('abort', onAbort)
        resolve()
      },
      (error: unknown) => {
        signal.removeEventListener('abort', onAbort)
        reject(error)
      }
    )
  })

// Batch form is a single array of `{ data, … }` items. (The one ambiguous case is a payload
// that is itself such an array; pass `[{ data: thatArray }]` to force the single form.)
const isBatch = (arg: unknown): arg is ReadonlyArray<ProduceMessage<unknown>> =>
  Array.isArray(arg) && arg.every((el) => typeof el === 'object' && el !== null && 'data' in el)

/**
 * The produce entry point: runs the `produce` event hook per message, the produce middleware
 * chain, then encodes and sends. One call is ONE adapter `send`.
 */
export function createProducer<W, Ctx = unknown>(
  adapter: Pick<ProducerAdapter<W, Ctx>, 'send' | 'delayedDelivery'>,
  codec: Codec<W>,
  opts: {
    readonly events?: Pick<QueueEvents, 'produce'>
    readonly middleware?: readonly QueueMiddleware[]
    /** Inline dispatch for `ProduceCall.invoke`. */
    readonly invoke: InvokeApi['invoke']
    /** Default bound (ms) on every produce call; composed with per-call signals. */
    readonly produceTimeout?: number
    readonly queues: QueuesView
  }
): ProducerApi {
  const { events, invoke, produceTimeout, queues } = opts
  const getQueue: ProduceCall['getQueue'] = (name) => queues.get(name)

  // The terminal stage, always innermost: encode whatever the chain passed down, apply the
  // bound, and send.
  const terminal: ProduceNext = async (call) => {
    if (call.messages.some((m) => m.delayMs !== undefined) && adapter.delayedDelivery !== true) {
      throw new NotSupportedError('delay')
    }
    const encoded = call.messages.map(
      (m): ProduceMessage<W> => ({
        ...pickMessage(m),
        data: codec.encode(m.data),
        headers: m.headers ?? {},
      })
    )
    // A fresh timer per entry, so a retry middleware re-entering the terminal gets a full
    // bound per attempt.
    const timer = produceTimeout === undefined ? undefined : AbortSignal.timeout(produceTimeout)
    const bound =
      timer !== undefined && call.signal !== undefined
        ? AbortSignal.any([call.signal, timer])
        : (timer ?? call.signal)
    // A signal already dead short-circuits: the one provably-not-sent abort.
    if (bound?.aborted === true) throw bound.reason
    const context: SendContext<Ctx> = {
      ...(call.ctx !== undefined ? { ctx: call.ctx as Ctx } : {}),
      queues,
    }
    const pending = adapter.send(call.queue, encoded, context, bound)
    await (bound === undefined ? pending : raceAbort(pending, bound))
  }
  const chain = produceStages(opts.middleware).reduceRight((next, mw) => mw(next), terminal)

  const sendMany = async (
    queue: string,
    messages: ReadonlyArray<ProduceMessage<unknown>>,
    options: ProduceBatchOptions<unknown> | undefined
  ): Promise<void> => {
    if (messages.length === 0) return
    // Header enrichment runs BEFORE the chain, so middleware sees the batch as it would go on
    // the wire. Headers are copied, so neither the hook nor a middleware mutates caller state.
    await chain({
      queue,
      messages: messages.map((m) => {
        const headers: Record<string, string> = { ...m.headers }
        events?.produce?.({ queue, headers, ...(m.key !== undefined ? { key: m.key } : {}) })
        return { ...pickMessage(m), headers }
      }),
      ...(options?.signal !== undefined ? { signal: options.signal } : {}),
      ...(options?.ctx !== undefined ? { ctx: options.ctx } : {}),
      invoke,
      getQueue,
    })
  }

  const produceInstances = async (
    messages: MessageInstance | ReadonlyArray<MessageInstance>,
    options?: ProduceBatchOptions<unknown>
  ): Promise<void> => {
    const list: readonly unknown[] = isMessageInstance(messages) ? [messages] : messages
    if (!Array.isArray(list)) {
      throw new TypeError(
        'queue: produce expects a queue name, a message instance, or an array of message instances'
      )
    }
    let queue: string | undefined
    for (const m of list) {
      if (!isMessageInstance(m)) {
        throw new TypeError(
          'queue: every element of a message batch must be a defineMessage instance'
        )
      }
      const resolved = resolveMessageQueue(queues.classIndex, m)
      // One produce call is one adapter send (atomic on transactional backends), so a
      // mixed-queue batch is rejected rather than silently split.
      if (queue !== undefined && resolved !== queue) {
        throw new Error(`queue: mixed-queue batch — "${resolved}" alongside "${queue}"`)
      }
      queue = resolved
    }
    if (queue === undefined) return
    await sendMany(queue, (list as readonly MessageInstance[]).map(toProduceMessage), options)
  }

  const produce: ProducerApi['produce'] = async (queueOrMessages, dataOrMessages, options) => {
    if (typeof queueOrMessages !== 'string') {
      return produceInstances(queueOrMessages, dataOrMessages as ProduceBatchOptions<unknown>)
    }
    const { ctx, signal, ...perMessage } = options ?? {}
    const single = pickMessage({ ...perMessage, data: dataOrMessages })
    // A batch is an array of `{ data }` items whose options carry no per-message fields.
    const batch = Object.keys(single).length === 1 && isBatch(dataOrMessages)
    await sendMany(queueOrMessages, batch ? dataOrMessages : [single], options)
  }

  return { produce }
}
