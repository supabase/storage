/**
 * A queue's retry/dead-letter policy. Enforcement is the ADAPTER's: the backend counts the
 * budget in its own unit (explicit retries, or deliveries where nack and crash redelivery
 * also consume) and dead-letters exhausted messages itself. The manager only computes each
 * retry's delay ({@link retryDelayOf}). The backoff applies to handler FAILURES only (a throw,
 * or running out of `consumeTimeout`): a shutdown release and a crash redelivery never go
 * through it and keep the engine's timing.
 */
export interface RetryPolicy {
  /** Max delivery attempts before the message is dead-lettered (or dropped, sans `deadLetter`). */
  readonly maxAttempts: number
  /** Queue that receives exhausted messages, provisioned before this one. Omit to drop them. */
  readonly deadLetter?: string
  /** Delay (ms) before a failed message is redelivered. Defaults to 0. */
  readonly backoffMs?: number
  /** `'fixed'` (default): every retry waits `backoffMs`. `'exponential'`: a message that
   * failed on attempt `n` (1-based) waits `min(backoffMs * 2^(n-1), maxBackoffMs)`. */
  readonly backoff?: 'fixed' | 'exponential'
  /** Ceiling (ms) on the delay. At least `backoffMs`. Omit for no ceiling. */
  readonly maxBackoffMs?: number
}

/** The delay (ms) before redelivering a message that failed on `attempt` (1-based), under
 * `policy`. The one place the backoff curve is computed. */
export const retryDelayOf = (policy: RetryPolicy | undefined, attempt: number): number => {
  const base = policy?.backoffMs ?? 0
  if (base === 0) return 0
  const delay = policy?.backoff === 'exponential' ? base * 2 ** (Math.max(1, attempt) - 1) : base
  return Math.min(delay, policy?.maxBackoffMs ?? Number.MAX_SAFE_INTEGER)
}

/** The liveness window of a queue that declares none. */
export const DEFAULT_LIVENESS_TIMEOUT_MS = 300_000

/**
 * A queue's declared options: its policy, plus adapter-specific options in their own
 * namespace (`pgboss`, `sqs`), added to this interface by declaration merging in the
 * adapter's module.
 *
 * The policy lives on the queue, not on its handler, because the engines keep it there: it
 * is in place from provisioning, before anything is sent, on nodes that only produce too.
 */
export interface QueueOptions {
  /** Absent means no budget: failures are redelivered up to the adapter's own default. */
  readonly retry?: RetryPolicy
  /**
   * How long a delivery may go without being settled or heartbeated before the backend
   * treats its consumer as hung and redelivers it. Keep it short for fast crash takeover and
   * heartbeat through long jobs. Defaults to {@link DEFAULT_LIVENESS_TIMEOUT_MS}.
   */
  readonly livenessTimeoutMs?: number
}

/** A queue's liveness window, declared or default. */
export const livenessOf = (options: QueueOptions | undefined): number =>
  options?.livenessTimeoutMs ?? DEFAULT_LIVENESS_TIMEOUT_MS

declare const PHANTOM: unique symbol

/** The constructor shape of a message class (built with `defineMessage`). */
export type AnyMessageClass = abstract new (...args: never[]) => { readonly data: unknown }

/** The payload type bound to a message class. */
export type MessagePayload<C> = C extends abstract new (
  ...args: never[]
) => { readonly data: infer T }
  ? T
  : never

/**
 * A queue descriptor: binds a queue name to a payload type `T` (phantom, type-level only),
 * its provisioning options and, when declared with `queue(Class)`, its message classes.
 */
export interface QueueDescriptor<T> {
  readonly [PHANTOM]: T
  readonly options?: QueueOptions
  /** The message classes bound to this queue. Each class may be bound to one queue only. */
  readonly classes?: readonly AnyMessageClass[]
}

/** A queue descriptor with message classes bound to it: instances of a bound class produce
 * here, and handlers built over the class attach here. */
export interface ClassQueue<T, Cs extends readonly AnyMessageClass[]> extends QueueDescriptor<T> {
  readonly classes: Cs
}

/**
 * Declare a queue:
 *
 * - `queue<T>(options?)`: a plain queue with payload type `T`.
 * - `queue(MessageClass, options?)`: bind one message class.
 * - `queue([ClassA, ClassB], options?)`: a polymorphic queue, payload type their union.
 */
export function queue<C extends AnyMessageClass>(
  cls: C,
  options?: QueueOptions
): ClassQueue<MessagePayload<C>, readonly [C]>
export function queue<const Cs extends readonly AnyMessageClass[]>(
  classes: Cs,
  options?: QueueOptions
): ClassQueue<MessagePayload<Cs[number]>, Cs>
export function queue<T>(options?: QueueOptions): QueueDescriptor<T>
export function queue(
  arg?: AnyMessageClass | readonly AnyMessageClass[] | QueueOptions,
  options?: QueueOptions
): QueueDescriptor<unknown> {
  const withOptions = options !== undefined ? { options } : {}
  if (typeof arg === 'function') {
    return { classes: [arg], ...withOptions } as unknown as QueueDescriptor<unknown>
  }
  if (Array.isArray(arg)) {
    return { classes: arg, ...withOptions } as unknown as QueueDescriptor<unknown>
  }
  return (arg !== undefined ? { options: arg } : {}) as QueueDescriptor<unknown>
}

export type QueueMap = Record<string, QueueDescriptor<unknown>>

/** The payload type bound to a queue descriptor. */
export type PayloadOf<Q> = Q extends QueueDescriptor<infer T> ? T : never

/**
 * The declared queue registry: every queue name the manager knows, bound to its payload type
 * and options. Built by {@link defineQueues}; the manager is constructed over it and hands it
 * to its adapter, so both read one declaration. A message class bound to two queues throws
 * here, before any manager exists.
 */
export class QueueRegistry<M extends QueueMap = QueueMap> {
  readonly #queues: M
  readonly #classIndex: ReadonlyMap<AnyMessageClass, string>

  constructor(queues: M) {
    this.#queues = queues
    this.#classIndex = buildClassIndex(queues)
  }

  /** Look up a declared queue. A name the registry's type knows returns its exact descriptor;
   * any other string resolves to `QueueDescriptor<unknown> | undefined`. */
  get<K extends keyof M & string>(name: K): string extends K ? M[K] | undefined : M[K]
  get(name: string): QueueDescriptor<unknown> | undefined
  get(name: string): QueueDescriptor<unknown> | undefined {
    return (this.#queues as QueueMap)[name]
  }

  /** Every declared queue name, in declaration order. */
  names(): readonly (keyof M & string)[] {
    return Object.keys(this.#queues) as (keyof M & string)[]
  }

  /** The declared map, verbatim. */
  toMap(): M {
    return this.#queues
  }

  /** Message class to queue name, for every class bound via `queue(Class)`. */
  get classIndex(): ReadonlyMap<AnyMessageClass, string> {
    return this.#classIndex
  }
}

/**
 * The map-agnostic read surface of a {@link QueueRegistry}: what adapters and manager
 * internals accept. Registries with different maps are not mutually assignable, but every
 * one of them is assignable here.
 */
export interface QueuesView {
  get(name: string): QueueDescriptor<unknown> | undefined
  names(): readonly string[]
  readonly classIndex: ReadonlyMap<AnyMessageClass, string>
}

/** What a manager's queues generic accepts: the registry `defineQueues` returns, or a raw map. */
export type QueuesSource = QueueMap | QueuesView

/** Normalize a {@link QueuesSource} to its declared map. */
export type QueueMapOf<In extends QueuesSource> =
  In extends QueueRegistry<infer M> ? M : In extends QueueMap ? In : never

/** Declare the queue registry. Throws if one message class is bound to two queues. */
export function defineQueues<const M extends QueueMap>(queues: M): QueueRegistry<M> {
  return new QueueRegistry(queues)
}

function buildClassIndex(queues: QueueMap): Map<AnyMessageClass, string> {
  const index = new Map<AnyMessageClass, string>()
  for (const name of Object.keys(queues)) {
    for (const cls of queues[name]?.classes ?? []) {
      const existing = index.get(cls)
      if (existing !== undefined && existing !== name) {
        throw new Error(
          `queue: message class "${cls.name}" is bound to both "${existing}" and "${name}" — a class may be bound to one queue only`
        )
      }
      index.set(cls, name)
    }
  }
  return index
}

/** Resolve a message class's queue, walking the prototype chain so a subclass of a bound
 * class still resolves. `undefined` when nothing in the chain is bound. */
export function resolveClassQueue(
  index: ReadonlyMap<AnyMessageClass, string>,
  cls: AnyMessageClass
): string | undefined {
  let ctor: unknown = cls
  while (typeof ctor === 'function') {
    const name = index.get(ctor as AnyMessageClass)
    if (name !== undefined) return name
    ctor = Object.getPrototypeOf(ctor)
  }
  return undefined
}

/** Resolve a message instance's queue. Throws (naming the class) when it is not bound. */
export function resolveMessageQueue(
  index: ReadonlyMap<AnyMessageClass, string>,
  instance: object
): string {
  const name = instance.constructor?.name ?? '?'
  const resolved = resolveClassQueue(index, instance.constructor as AnyMessageClass)
  if (resolved === undefined) {
    throw new Error(
      `queue: message class "${name}" is not bound to any queue — bind it in the registry with queue(${name})`
    )
  }
  return resolved
}
