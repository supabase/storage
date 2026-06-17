import type { Attributes, BatchObservableCallback, Meter, Observable } from '@opentelemetry/api'
import { LRUCache } from 'lru-cache'

type BatchObservableObserver = Parameters<BatchObservableCallback>[0]

type RegisterCounterMetric = <T>(name: string, type: 'counter', factory: () => T) => T

type ObservableCounterConfig = {
  name: string
  description: string
  unit?: string
}

type DefaultObservableCounterState<TCounter extends string> = Record<TCounter, number> & {
  attributes: Attributes
}

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

function observeDefaultCounters<TCounter extends string>(
  observer: BatchObservableObserver,
  counters: Record<TCounter, Observable>,
  counterKeys: TCounter[],
  state: DefaultObservableCounterState<TCounter>
): void {
  for (const counterKey of counterKeys) {
    const value = state[counterKey]

    if (value > 0) {
      observer.observe(counters[counterKey], value, state.attributes)
    }
  }
}

export function createBatchObservableCounterGroup<
  TCounter extends string,
  TInput,
  TKey extends {},
  TState extends DefaultObservableCounterState<TCounter>,
>(
  options: BatchObservableCounterGroupOptions<TCounter, TInput, TKey, TState>
): BatchObservableCounterGroup<TInput, TState>

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
): BatchObservableCounterGroup<TInput, TState> {
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
        state as unknown as DefaultObservableCounterState<TCounter>
      ))

  options.meter.addBatchObservableCallback(
    (observer) => {
      states.forEach((state) => observe(observer, counters, state))
    },
    counterKeys.map((counterKey) => counters[counterKey])
  )

  return {
    state(input) {
      const key = options.getKey(input)
      let state = states.get(key)

      if (!state) {
        state = options.createState(input)
        states.set(key, state)
      }

      return state
    },
  }
}
