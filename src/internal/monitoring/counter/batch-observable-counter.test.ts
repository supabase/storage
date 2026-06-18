import type { Attributes, BatchObservableCallback, Meter, Observable } from '@opentelemetry/api'
import { describe, expect, test, vi } from 'vitest'

import {
  createBatchObservableCounterGroup,
  type ObservableCounterSeries,
  SAFE_COUNTER_THRESHOLD,
} from './batch-observable-counter'

interface Observation {
  name: string
  value: number
  attributes?: Attributes
}

type CounterOptions = { description?: string; unit?: string }

function createFakeMeter() {
  const counterNames = new Map<Observable, string>()
  const createdCounters: { name: string; options?: CounterOptions }[] = []
  const batchCallbacks: { callback: BatchObservableCallback; observables: Observable[] }[] = []

  const meter = {
    createObservableCounter: vi.fn((name: string, options?: CounterOptions) => {
      const observable = { name } as unknown as Observable
      counterNames.set(observable, name)
      createdCounters.push({ name, options })
      return observable
    }),
    addBatchObservableCallback: vi.fn(
      (callback: BatchObservableCallback, observables: Observable[]) => {
        batchCallbacks.push({ callback, observables })
      }
    ),
  } as unknown as Meter

  /** Runs every registered batch callback and returns the resulting observations in order. */
  const collect = (): Observation[] => {
    const observations: Observation[] = []
    for (const { callback } of batchCallbacks) {
      void callback({
        observe: (observable, value, attributes) => {
          observations.push({ name: counterNames.get(observable) as string, value, attributes })
        },
      })
    }
    return observations
  }

  return { meter, createdCounters, batchCallbacks, collect }
}

const registerMetric = <T>(_name: string, _type: 'counter', factory: () => T): T => factory()

interface UploadState {
  started: ObservableCounterSeries
  success: ObservableCounterSeries
}

/** A representative two-counter group keyed by a plain string, mirroring the upload metrics. */
function createUploadGroup(meter: Meter, maxStates = 10) {
  const createState = vi.fn(
    (uploadType: string): UploadState => ({
      started: { count: 0, attributes: { uploadType } },
      success: { count: 0, attributes: { uploadType } },
    })
  )

  const group = createBatchObservableCounterGroup({
    meter,
    registerMetric,
    maxStates,
    counters: {
      started: { name: 'upload_started', description: 'Total uploads started' },
      success: { name: 'upload_success', description: 'Total successful uploads' },
    },
    getKey: (uploadType: string) => uploadType,
    createState,
  })

  return { group, createState }
}

describe('createBatchObservableCounterGroup', () => {
  test('registers one observable counter per config and a single batch callback over all of them', () => {
    const meter = createFakeMeter()
    createUploadGroup(meter.meter)

    expect(meter.createdCounters.map((counter) => counter.name)).toEqual([
      'upload_started',
      'upload_success',
    ])
    expect(meter.batchCallbacks).toHaveLength(1)
    expect(meter.batchCallbacks[0].observables).toHaveLength(2)
  })

  test('forwards description and unit to each counter, omitting unit when absent', () => {
    const meter = createFakeMeter()

    createBatchObservableCounterGroup({
      meter: meter.meter,
      registerMetric,
      maxStates: 10,
      counters: {
        bytes: { name: 'request_bytes', description: 'bytes in', unit: 'bytes' },
        count: { name: 'request_total', description: 'requests' },
      },
      getKey: (key: string) => key,
      createState: (key: string) => ({ key }),
      observe: () => undefined,
    })

    expect(meter.createdCounters).toEqual([
      { name: 'request_bytes', options: { description: 'bytes in', unit: 'bytes' } },
      { name: 'request_total', options: { description: 'requests' } },
    ])
  })

  test('state() creates one tally per derived key and reuses it for equal keys', () => {
    const meter = createFakeMeter()
    const { group, createState } = createUploadGroup(meter.meter)

    const first = group.state('standard')
    const second = group.state('standard')
    const other = group.state('multipart')

    expect(second).toBe(first)
    expect(other).not.toBe(first)
    expect(createState).toHaveBeenCalledTimes(2)
  })

  test('dedups by derived key even when inputs are different objects', () => {
    const meter = createFakeMeter()
    const createState = vi.fn((input: { method: string; status: string }) => ({
      count: 0,
      attributes: { method: input.method, status: input.status } as Attributes,
    }))

    const group = createBatchObservableCounterGroup({
      meter: meter.meter,
      registerMetric,
      maxStates: 10,
      counters: { total: { name: 'http_total', description: 'requests' } },
      getKey: (input: { method: string; status: string }) => `${input.method}\x00${input.status}`,
      createState,
      observe: (observer, counters, state) => {
        if (state.count > 0) {
          observer.observe(counters.total, state.count, state.attributes)
        }
      },
    })

    group.state({ method: 'GET', status: '200' }).count++
    group.state({ method: 'GET', status: '200' }).count++

    expect(createState).toHaveBeenCalledTimes(1)
    expect(meter.collect()).toEqual([
      { name: 'http_total', value: 2, attributes: { method: 'GET', status: '200' } },
    ])
  })

  test('default observer collects each state once and routes every series to its counter', () => {
    const meter = createFakeMeter()
    const { group } = createUploadGroup(meter.meter)

    group.state('standard').started.count += 2
    group.state('standard').success.count += 1
    group.state('multipart').started.count += 1

    const observations = meter.collect()

    expect(observations).toHaveLength(3)
    expect(observations).toEqual(
      expect.arrayContaining([
        { name: 'upload_started', value: 2, attributes: { uploadType: 'standard' } },
        { name: 'upload_success', value: 1, attributes: { uploadType: 'standard' } },
        { name: 'upload_started', value: 1, attributes: { uploadType: 'multipart' } },
      ])
    )
  })

  test('default observer omits zero-value series', () => {
    const meter = createFakeMeter()
    const { group } = createUploadGroup(meter.meter)

    group.state('standard').started.count += 1

    expect(meter.collect()).toEqual([
      { name: 'upload_started', value: 1, attributes: { uploadType: 'standard' } },
    ])
  })

  test('generated counter methods increment counter series without an update object', () => {
    const meter = createFakeMeter()
    const { group, createState } = createUploadGroup(meter.meter)

    group.addStarted('standard')
    group.addSuccess('standard', 2)
    group.addStarted('standard', 0)
    group.addSuccess('standard', Number.NaN)

    expect(createState).toHaveBeenCalledTimes(1)
    expect(meter.collect()).toEqual(
      expect.arrayContaining([
        { name: 'upload_started', value: 1, attributes: { uploadType: 'standard' } },
        { name: 'upload_success', value: 2, attributes: { uploadType: 'standard' } },
      ])
    )
  })

  test('add() keeps flat counters below the safe threshold', () => {
    const meter = createFakeMeter()
    const { group } = createUploadGroup(meter.meter)

    group.addStarted('standard', SAFE_COUNTER_THRESHOLD - 1)
    group.add('standard', 'started', 2)

    expect(meter.collect()).toEqual([
      {
        name: 'upload_started',
        value: SAFE_COUNTER_THRESHOLD,
        attributes: { uploadType: 'standard' },
      },
    ])
  })

  test('generated counter methods increment nested counter maps', () => {
    const meter = createFakeMeter()
    const group = createBatchObservableCounterGroup({
      meter: meter.meter,
      registerMetric,
      maxStates: 10,
      counters: {
        requests: { name: 'cache_requests_total', description: 'cache requests' },
        evictions: { name: 'cache_evictions_total', description: 'cache evictions' },
      },
      getKey: (cache: string) => cache,
      createState: (cache: string) => ({
        requests: {
          hit: { count: 0, attributes: { cache, outcome: 'hit' } as Attributes },
          miss: { count: 0, attributes: { cache, outcome: 'miss' } as Attributes },
        },
        evictions: { count: 0, attributes: { cache } as Attributes },
      }),
    })

    group.addRequests('metadata', 'hit')
    group.addRequests('metadata', 'miss', 2)
    group.addEvictions('metadata')

    expect(meter.collect()).toEqual([
      {
        name: 'cache_requests_total',
        value: 1,
        attributes: { cache: 'metadata', outcome: 'hit' },
      },
      {
        name: 'cache_requests_total',
        value: 2,
        attributes: { cache: 'metadata', outcome: 'miss' },
      },
      { name: 'cache_evictions_total', value: 1, attributes: { cache: 'metadata' } },
    ])
  })

  test('evicts least recently used states once maxStates is reached', () => {
    const meter = createFakeMeter()
    const { group, createState } = createUploadGroup(meter.meter, 2)

    group.state('standard').started.count += 1
    group.state('multipart').started.count += 1
    group.state('standard').success.count += 1
    group.state('resumable').started.count += 1

    const observations = meter.collect()

    expect(createState).toHaveBeenCalledTimes(3)
    expect(observations).toHaveLength(3)
    expect(observations).toEqual(
      expect.arrayContaining([
        { name: 'upload_started', value: 1, attributes: { uploadType: 'standard' } },
        { name: 'upload_success', value: 1, attributes: { uploadType: 'standard' } },
        { name: 'upload_started', value: 1, attributes: { uploadType: 'resumable' } },
      ])
    )
    expect(
      observations.some((observation) => observation.attributes?.uploadType === 'multipart')
    ).toBe(false)

    const recreated = group.state('multipart')

    expect(recreated.started.count).toBe(0)
    expect(recreated.success.count).toBe(0)
    expect(createState).toHaveBeenCalledTimes(4)
  })

  test('requires a positive maxStates cap', () => {
    const meter = createFakeMeter()

    expect(() =>
      createBatchObservableCounterGroup({
        meter: meter.meter,
        registerMetric,
        maxStates: 0,
        counters: { total: { name: 'http_total', description: 'requests' } },
        getKey: (key: string) => key,
        createState: () => ({ count: 0 }),
        observe: () => undefined,
      })
    ).toThrow('maxStates must be a positive integer')
  })
})
