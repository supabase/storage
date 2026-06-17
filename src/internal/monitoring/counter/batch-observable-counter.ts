import type { BatchObservableCallback, Meter, Observable } from '@opentelemetry/api'

type BatchObservableObserver = Parameters<BatchObservableCallback>[0]

type RegisterCounterMetric = <T>(name: string, type: 'counter', factory: () => T) => T

type ObservableCounterConfig = {
  name: string
  description: string
  unit?: string
}

type BatchObservableCounterGroup<TInput, TState> = {
  /** Returns the mutable tally for `input`, creating it on first use. */
  state(input: TInput): TState
}

export function createBatchObservableCounterGroup<
  TCounter extends string,
  TInput,
  TKey,
  TState,
>(options: {
  meter: Meter
  registerMetric: RegisterCounterMetric
  counters: Record<TCounter, ObservableCounterConfig>
  getKey(input: TInput): TKey
  createState(input: TInput): TState
  observe(
    observer: BatchObservableObserver,
    counters: Record<TCounter, Observable>,
    state: TState
  ): void
}): BatchObservableCounterGroup<TInput, TState> {
  const states = new Map<TKey, TState>()
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

  options.meter.addBatchObservableCallback(
    (observer) => {
      states.forEach((state) => options.observe(observer, counters, state))
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
