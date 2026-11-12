import {
  json,
  type ProduceMessage,
  type ProducerAdapter,
  type QueueOptions,
  type SendContext,
} from './index'

/** Resolve after `ms` milliseconds. */
export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

/** A promise with its settle functions exposed, for scripting an exact interleaving. */
export const deferred = <T = void>(): {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (error: unknown) => void
} => {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

/** Await a promise that must reject and return what it rejected with (identity preserved). */
export const rejection = (promise: Promise<unknown>): Promise<unknown> =>
  promise.then(
    () => {
      throw new Error('expected a rejection, but the promise resolved')
    },
    (error: unknown) => error
  )

/** Poll until `pred` holds. */
export const until = async (pred: () => boolean, ms = 2_000): Promise<void> => {
  const deadline = Date.now() + ms
  while (!pred()) {
    if (Date.now() > deadline) throw new Error('until: timed out')
    await sleep(5)
  }
}

/** A send that never settles: the hung-backend case the produce bound exists for. */
export const hangingSend = (): Promise<void> => new Promise<never>(() => {})

export interface RecordedSend<Ctx = never> {
  readonly queue: string
  readonly messages: readonly ProduceMessage<unknown>[]
  readonly context: SendContext<Ctx>
  readonly signal: AbortSignal | undefined
}

export interface RecordingAdapter<Ctx = never> {
  readonly adapter: ProducerAdapter<unknown, Ctx>
  /** Every `send` the manager issued, in order. */
  readonly sends: RecordedSend<Ctx>[]
  /** Every `provision` call, in order. */
  readonly provisioned: { queue: string; options: QueueOptions | undefined }[]
  /** The lifecycle calls in the order they happened: `start`, `provision:<queue>`, `close`. */
  readonly log: string[]
}

/**
 * A producer-only backend double: exactly a `ProducerAdapter`, recording what the manager
 * hands it. `overrides` replace individual members (a failing `send`, a `stats`, ...).
 */
export function recordingAdapter<Ctx = never>(
  overrides?: Partial<ProducerAdapter<unknown, Ctx>>
): RecordingAdapter<Ctx> {
  const sends: RecordedSend<Ctx>[] = []
  const provisioned: { queue: string; options: QueueOptions | undefined }[] = []
  const log: string[] = []
  const adapter: ProducerAdapter<unknown, Ctx> = {
    name: 'recording',
    codec: json(),
    delayedDelivery: true,
    async start() {
      log.push('start')
    },
    async close() {
      log.push('close')
    },
    async provision(queue, options) {
      log.push(`provision:${queue}`)
      provisioned.push({ queue, options })
    },
    async send(queue, messages, context, signal) {
      sends.push({ queue, messages, context, signal })
    },
    ...overrides,
  }
  return { adapter, sends, provisioned, log }
}
