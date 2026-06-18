import type { Attributes, BatchObservableCallback, Meter, Observable } from '@opentelemetry/api'
import { LRUCache } from 'lru-cache'

type BatchObservableObserver = Parameters<BatchObservableCallback>[0]

type RegisterCounterMetric = <T>(name: string, type: 'counter', factory: () => T) => T

type ObservableCounterConfig = {
  name: string
  description: string
  unit?: string
}

type AddCounterMethodName<TCounter extends string> = `add${Capitalize<TCounter>}`

export type ObservableCounterSeries = {
  count: number
  attributes: Attributes
}

type CounterDimension<TValue> = TValue extends object
  ? {
      [TKey in keyof TValue & string]: TValue[TKey] extends ObservableCounterSeries ? TKey : never
    }[keyof TValue & string]
  : never

type AddCounterMethod<TInput, TValue> = TValue extends ObservableCounterSeries
  ? (input: TInput, amount?: number) => void
  : CounterDimension<TValue> extends never
    ? never
    : (input: TInput, dimension: CounterDimension<TValue>, amount?: number) => void

type AutoAddCounterMethods<
  TCounter extends string,
  TInput,
  TState extends Record<TCounter, unknown>,
> = {
  [TKey in TCounter as AddCounterMethod<TInput, TState[TKey]> extends never
    ? never
    : AddCounterMethodName<TKey>]: AddCounterMethod<TInput, TState[TKey]>
}

type ObservableCounterGroupState<TCounter extends string> = Record<TCounter, unknown>

type BatchObservableCounterGroup<TInput, TState> = {
  /** Returns the mutable tally for `input`, creating it on first use. */
  state(input: TInput): TState
}

type BatchObservableCounterGroupOptions<
  TCounter extends string,
  TInput,
  TKey extends {},
  TState extends {},
> = {
  meter: Meter
  registerMetric: RegisterCounterMetric
  counters: Record<TCounter, ObservableCounterConfig>
  maxStates: number
  getKey(input: TInput): TKey
  createState(input: TInput): TState
}

type CustomObservableCounterGroupOptions<
  TCounter extends string,
  TInput,
  TKey extends {},
  TState extends {},
> = BatchObservableCounterGroupOptions<TCounter, TInput, TKey, TState> & {
  observe(
    observer: BatchObservableObserver,
    counters: Record<TCounter, Observable>,
    state: TState
  ): void
}

export const SAFE_COUNTER_THRESHOLD = Number.MAX_SAFE_INTEGER - 1_000_000_000

export function safeAddCounter(current: number, amount: number): number {
  const next = current + amount

  if (
    current >= SAFE_COUNTER_THRESHOLD ||
    next >= SAFE_COUNTER_THRESHOLD ||
    !Number.isSafeInteger(next)
  ) {
    return SAFE_COUNTER_THRESHOLD
  }

  return next
}

function isAddableCounterAmount(amount: number): boolean {
  return Number.isFinite(amount) && amount > 0
}

function addMethodName(counterKey: string): `add${string}` {
  return `add${counterKey.charAt(0).toUpperCase()}${counterKey.slice(1)}`
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object')
}

function isObservableCounterState(value: unknown): value is ObservableCounterSeries {
  return isRecord(value) && typeof value.count === 'number' && isRecord(value.attributes)
}

function observeDefaultCounters<
  TCounter extends string,
  TState extends ObservableCounterGroupState<TCounter>,
>(
  observer: BatchObservableObserver,
  counters: Record<TCounter, Observable>,
  counterKeys: TCounter[],
  state: TState
): void {
  for (const counterKey of counterKeys) {
    const value = state[counterKey]

    if (isObservableCounterState(value) && value.count > 0) {
      observer.observe(counters[counterKey], value.count, value.attributes)
      continue
    }

    if (!isRecord(value)) {
      continue
    }

    for (const key in value) {
      const series = value[key]
      if (isObservableCounterState(series) && series.count > 0) {
        observer.observe(counters[counterKey], series.count, series.attributes)
      }
    }
  }
}

function addCounterState(counterState: ObservableCounterSeries, amount = 1): void {
  if (isAddableCounterAmount(amount)) {
    counterState.count = safeAddCounter(counterState.count, amount)
  }
}

export function createBatchObservableCounterGroup<
  TCounter extends string,
  TInput,
  TKey extends {},
  TState extends ObservableCounterGroupState<TCounter>,
>(
  options: BatchObservableCounterGroupOptions<TCounter, TInput, TKey, TState> & {
    observe?: CustomObservableCounterGroupOptions<TCounter, TInput, TKey, TState>['observe']
  }
): BatchObservableCounterGroup<TInput, TState> & {
  add(input: TInput, counterKey: TCounter, amount?: number): void
} & AutoAddCounterMethods<TCounter, TInput, TState>

export function createBatchObservableCounterGroup<
  TCounter extends string,
  TInput,
  TKey extends {},
  TState extends {},
>(
  options: CustomObservableCounterGroupOptions<TCounter, TInput, TKey, TState>
): BatchObservableCounterGroup<TInput, TState>

export function createBatchObservableCounterGroup<
  TCounter extends string,
  TInput,
  TKey extends {},
  TState extends {},
>(
  options: BatchObservableCounterGroupOptions<TCounter, TInput, TKey, TState> & {
    observe?: CustomObservableCounterGroupOptions<TCounter, TInput, TKey, TState>['observe']
  }
): Record<string, unknown> {
  if (!Number.isInteger(options.maxStates) || options.maxStates < 1) {
    throw new Error('maxStates must be a positive integer')
  }

  const states = new LRUCache<TKey, TState>({ max: options.maxStates })
  const counterKeys = Object.keys(options.counters) as TCounter[]
  const counters = {} as Record<TCounter, Observable>

  for (const counterKey of counterKeys) {
    const config = options.counters[counterKey]
    counters[counterKey] = options.registerMetric(config.name, 'counter', () =>
      options.meter.createObservableCounter(
        config.name,
        config.unit
          ? { description: config.description, unit: config.unit }
          : { description: config.description }
      )
    )
  }

  const observe =
    options.observe ??
    ((observer: BatchObservableObserver, counters: Record<TCounter, Observable>, state: TState) =>
      observeDefaultCounters(
        observer,
        counters,
        counterKeys,
        state as unknown as ObservableCounterGroupState<TCounter>
      ))

  options.meter.addBatchObservableCallback(
    (observer) => {
      states.forEach((state) => observe(observer, counters, state))
    },
    counterKeys.map((counterKey) => counters[counterKey])
  )

  const getState = (input: TInput): TState => {
    const key = options.getKey(input)
    let state = states.get(key)

    if (!state) {
      state = options.createState(input)
      states.set(key, state)
    }

    return state
  }

  const addCounter = (
    input: TInput,
    counterKey: TCounter,
    arg1?: unknown,
    arg2?: unknown
  ): void => {
    const state = getState(input) as Record<string, unknown>
    const value = state[counterKey]

    if (isObservableCounterState(value)) {
      const amount = typeof arg1 === 'number' ? arg1 : 1
      addCounterState(value, amount)
      return
    }

    if (!isRecord(value) || typeof arg1 !== 'string') {
      return
    }

    const countState = value[arg1]
    const amount = typeof arg2 === 'number' ? arg2 : 1

    if (isObservableCounterState(countState)) {
      addCounterState(countState, amount)
    }
  }

  const group = {
    state: getState,
    add: (input: TInput, counterKey: TCounter, amount = 1) => addCounter(input, counterKey, amount),
  } as BatchObservableCounterGroup<TInput, TState> & {
    add(input: TInput, counterKey: TCounter, amount?: number): void
  } & Record<string, unknown>

  for (const counterKey of counterKeys) {
    group[addMethodName(counterKey)] = (input: TInput, arg1?: unknown, arg2?: unknown) =>
      addCounter(input, counterKey, arg1, arg2)
  }

  return group
}
