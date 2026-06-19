import type { BatchObservableCallback, Meter, Observable } from '@opentelemetry/api'
import { beforeEach, describe, expect, test, vi } from 'vitest'
import { HTTP_SIZE_METRICS_MAX_STATES } from './metric-limits'

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

interface CapturedBatchObservable {
  callback: BatchObservableCallback
  observables: Observable[]
}

function createMockMeter(): {
  meter: Meter
  invoke: (name: string) => CapturedObservation[]
} {
  const observables: CapturedObservable[] = []
  const batchObservables: CapturedBatchObservable[] = []
  const observableNames = new Map<Observable, string>()
  const noopMetric = {
    add: vi.fn(),
    record: vi.fn(),
  }

  const createObservable = () => (name: string) => {
    const observable = {
      addCallback(callback: CapturedObservable['callback']) {
        observables.push({ name, callback })
      },
      removeCallback: vi.fn(),
    } as unknown as Observable

    observableNames.set(observable, name)

    return observable
  }

  const meter = {
    createCounter: vi.fn(() => noopMetric),
    createGauge: vi.fn(() => noopMetric),
    createHistogram: vi.fn(() => noopMetric),
    createObservableCounter: vi.fn(createObservable()),
    createObservableGauge: vi.fn(createObservable()),
    createUpDownCounter: vi.fn(() => noopMetric),
    addBatchObservableCallback: vi.fn(
      (callback: BatchObservableCallback, observables: Observable[]) => {
        batchObservables.push({ callback, observables })
      }
    ),
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

    for (const batchObservable of batchObservables) {
      const hasObservable = batchObservable.observables.some(
        (observable) => observableNames.get(observable) === name
      )

      if (!hasObservable) {
        continue
      }

      void batchObservable.callback({
        observe: (observable, value, attributes) => {
          if (observableNames.get(observable) === name) {
            observations.push({ value, attributes })
          }
        },
      })
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

  test('accumulates request and response bytes independently, even when disabled', async () => {
    const { metricsModule, mockMeter } = await importMetricsWithMockMeter()

    const method = 'POST'
    const operation = 'object.upload'
    const statusCode = 200
    const attributes = { method, operation, status_code: String(statusCode) }

    metricsModule.setMetricsEnabled([
      { name: 'http_request_size_bytes', enabled: false },
      { name: 'http_response_size_bytes', enabled: false },
    ])
    metricsModule.recordHttpRequestMetrics(0.001, 7, 2, method, operation, statusCode)
    metricsModule.recordHttpRequestMetrics(0.001, 3, undefined, method, operation, statusCode)
    metricsModule.recordHttpRequestMetrics(0.001, undefined, 5, method, operation, statusCode)

    expect(mockMeter.invoke('http_request_size_bytes')).toEqual([
      {
        value: 10,
        attributes,
      },
    ])

    expect(mockMeter.invoke('http_response_size_bytes')).toEqual([
      {
        value: 7,
        attributes,
      },
    ])
  })

  test('keeps http byte counters below the safe threshold', async () => {
    const { metricsModule, mockMeter } = await importMetricsWithMockMeter()
    const safeCounterThreshold = Number.MAX_SAFE_INTEGER - 1_000_000_000

    const method = 'POST'
    const operation = 'object.upload'
    const statusCode = 200
    const attributes = { method, operation, status_code: String(statusCode) }

    metricsModule.recordHttpRequestMetrics(
      0.001,
      safeCounterThreshold - 1,
      undefined,
      method,
      operation,
      statusCode
    )
    metricsModule.recordHttpRequestMetrics(0.001, 2, undefined, method, operation, statusCode)
    metricsModule.recordHttpRequestMetrics(
      0.001,
      undefined,
      Number.MAX_SAFE_INTEGER,
      method,
      operation,
      statusCode
    )

    expect(mockMeter.invoke('http_request_size_bytes')).toEqual([
      {
        value: safeCounterThreshold,
        attributes,
      },
    ])

    expect(mockMeter.invoke('http_response_size_bytes')).toEqual([
      {
        value: safeCounterThreshold,
        attributes,
      },
    ])
  })

  test('ignores invalid http byte sizes without poisoning later observations', async () => {
    const { metricsModule, mockMeter } = await importMetricsWithMockMeter()

    const method = 'POST'
    const operation = 'object.upload'
    const statusCode = 200
    const attributes = { method, operation, status_code: String(statusCode) }

    metricsModule.recordHttpRequestMetrics(
      0.001,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      method,
      operation,
      statusCode
    )
    metricsModule.recordHttpRequestMetrics(0.001, 7, 2, method, operation, statusCode)

    expect(mockMeter.invoke('http_request_size_bytes')).toEqual([
      {
        value: 7,
        attributes,
      },
    ])

    expect(mockMeter.invoke('http_response_size_bytes')).toEqual([
      {
        value: 2,
        attributes,
      },
    ])
  })

  test('routes new http metric states to overflow after the cap', async () => {
    const { metricsModule, mockMeter } = await importMetricsWithMockMeter()

    for (let i = 0; i <= HTTP_SIZE_METRICS_MAX_STATES; i++) {
      metricsModule.recordHttpRequestMetrics(0.001, 1, undefined, 'GET', `operation-${i}`, 200)
    }

    expect(mockMeter.invoke('http_request_size_bytes')).toEqual(
      expect.arrayContaining([
        {
          value: 1,
          attributes: {
            method: 'overflow',
            operation: 'overflow',
            status_code: 'overflow',
          },
        },
      ])
    )
  })

  test('observes cumulative upload counters with stable attributes even when disabled', async () => {
    const { metricsModule, mockMeter } = await importMetricsWithMockMeter()

    metricsModule.setMetricsEnabled([
      { name: 'upload_started', enabled: false },
      { name: 'upload_success', enabled: false },
    ])
    metricsModule.recordUploadStarted('standard')
    metricsModule.recordUploadStarted('standard')
    metricsModule.recordUploadStarted('multipart')
    metricsModule.recordUploadSuccess('standard')

    expect(mockMeter.invoke('upload_started')).toEqual([
      {
        value: 2,
        attributes: { uploadType: 'standard' },
      },
      {
        value: 1,
        attributes: { uploadType: 'multipart' },
      },
    ])

    expect(mockMeter.invoke('upload_success')).toEqual([
      {
        value: 1,
        attributes: { uploadType: 'standard' },
      },
    ])
  })
})
