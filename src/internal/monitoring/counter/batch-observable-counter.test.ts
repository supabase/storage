import type { Attributes, BatchObservableCallback, Meter, Observable } from '@opentelemetry/api'
import { describe, expect, test, vi } from 'vitest'

import { createBatchObservableCounterGroup } from './batch-observable-counter'

interface Observation {
  name: string
  value: number
  attributes?: Attributes
}

type CounterOptions = { description?: string; unit?: string }

/**
 * Minimal in-memory `Meter` stand-in. The factory only touches
 * `createObservableCounter` and `addBatchObservableCallback`, so the fake can be
 * tiny — and because the factory takes its dependencies by injection, no module
 * mocking is required.
 */
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
  started: number
  success: number
  attributes: Attributes
}

/** A representative two-counter group keyed by a plain string, mirroring the upload metrics. */
function createUploadGroup(meter: Meter, maxStates = 10) {
  const createState = vi.fn(
    (uploadType: string): UploadState => ({
      started: 0,
      success: 0,
      attributes: { uploadType },
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
    observe: (observer, counters, state: UploadState) => {
      if (state.started > 0) {
        observer.observe(counters.started, state.started, state.attributes)
      }
      if (state.success > 0) {
        observer.observe(counters.success, state.success, state.attributes)
      }
    },
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

  test('collect() observes each state once and routes every series to its counter', () => {
    const meter = createFakeMeter()
    const { group } = createUploadGroup(meter.meter)

    group.state('standard').started += 2
    group.state('standard').success += 1
    group.state('multipart').started += 1

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

  test('collect() omits series the observe callback guards out', () => {
    const meter = createFakeMeter()
    const { group } = createUploadGroup(meter.meter)

    group.state('standard').started += 1

    expect(meter.collect()).toEqual([
      { name: 'upload_started', value: 1, attributes: { uploadType: 'standard' } },
    ])
  })

  test('evicts least recently used states once maxStates is reached', () => {
    const meter = createFakeMeter()
    const { group, createState } = createUploadGroup(meter.meter, 2)

    group.state('standard').started += 1
    group.state('multipart').started += 1
    group.state('standard').success += 1
    group.state('resumable').started += 1

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

    expect(recreated.started).toBe(0)
    expect(recreated.success).toBe(0)
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
