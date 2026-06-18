import type { Meter } from '@opentelemetry/api'
import { beforeEach, describe, expect, test, vi } from 'vitest'

interface CapturedObservable {
  name: string
  callback: (observer: {
    observe: (value: number, attributes?: Record<string, unknown>) => void
  }) => void
}

interface CapturedObservation {
  value: number
  attributes?: Record<string, unknown>
}

function createMockMeter(): {
  meter: Meter
  invoke: (name: string) => CapturedObservation[]
} {
  const observables: CapturedObservable[] = []
  const noopMetric = {
    add: vi.fn(),
    record: vi.fn(),
  }

  const createObservable = () => (name: string) => ({
    addCallback(callback: CapturedObservable['callback']) {
      observables.push({ name, callback })
    },
  })

  const meter = {
    createCounter: vi.fn(() => noopMetric),
    createGauge: vi.fn(() => noopMetric),
    createHistogram: vi.fn(() => noopMetric),
    createObservableCounter: vi.fn(createObservable()),
    createObservableGauge: vi.fn(createObservable()),
    createUpDownCounter: vi.fn(() => noopMetric),
    addBatchObservableCallback: vi.fn(),
    removeBatchObservableCallback: vi.fn(),
  } as unknown as Meter

  const invoke = (name: string): CapturedObservation[] => {
    const observations: CapturedObservation[] = []
    const observer = {
      observe: (value: number, attributes?: Record<string, unknown>) => {
        observations.push({ value, attributes })
      },
    }

    for (const observable of observables) {
      if (observable.name === name) {
        observable.callback(observer)
      }
    }

    return observations
  }

  return { meter, invoke }
}

async function importMetricsWithMockMeter() {
  vi.resetModules()

  const mockMeter = createMockMeter()
  vi.doMock('@opentelemetry/api', () => ({
    metrics: {
      getMeter: vi.fn(() => mockMeter.meter),
    },
  }))

  const metricsModule = await import('./metrics')
  return { metricsModule, mockMeter }
}

describe('metrics registry', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
    vi.doUnmock('@opentelemetry/api')
  })

  test('observes cumulative cache counters with stable attributes even when disabled', async () => {
    const { metricsModule, mockMeter } = await importMetricsWithMockMeter()

    metricsModule.setMetricsEnabled([
      { name: 'cache_requests_total', enabled: false },
      { name: 'cache_evictions_total', enabled: false },
    ])
    metricsModule.recordCacheRequest('tenant_config', 'hit')
    metricsModule.recordCacheRequest('tenant_config', 'hit')
    metricsModule.recordCacheRequest('tenant_config', 'miss')
    metricsModule.recordCacheRequest('tenant_pool', 'stale')
    metricsModule.recordCacheEviction('tenant_config')

    expect(mockMeter.invoke('cache_requests_total')).toEqual([
      {
        value: 2,
        attributes: { cache: 'tenant_config', outcome: 'hit' },
      },
      {
        value: 1,
        attributes: { cache: 'tenant_config', outcome: 'miss' },
      },
      {
        value: 1,
        attributes: { cache: 'tenant_pool', outcome: 'stale' },
      },
    ])

    expect(mockMeter.invoke('cache_evictions_total')).toEqual([
      {
        value: 1,
        attributes: { cache: 'tenant_config' },
      },
    ])
  })
})
