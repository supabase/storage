import { PgTenantConnection } from '@internal/database'
import { dbQueryPerformance } from '@internal/monitoring/metrics'
import { vi } from 'vitest'
import { escapeLike, StoragePgDB } from './pg'

class TestStoragePgDB extends StoragePgDB {
  runMetricProbe(): Promise<string> {
    return this.runUnscopedQuery('MetricWithoutTenantAttribute', async () => 'ok')
  }
}

describe('escapeLike', () => {
  test('escapes SQL wildcard characters', () => {
    expect(escapeLike('%_abc')).toBe('\\%\\_abc')
    expect(escapeLike('a%b_c')).toBe('a\\%b\\_c')
    expect(escapeLike('plain-text')).toBe('plain-text')
  })
})

describe('StoragePgDB metrics', () => {
  test('records DB query duration without tenantId attribute', async () => {
    const connection = {
      getAbortSignal: vi.fn().mockReturnValue(undefined),
      pool: {
        acquire: vi.fn(),
      },
    } as unknown as PgTenantConnection
    const storage = new TestStoragePgDB(connection, {
      tenantId: 'metric-cardinality-tenant',
      host: 'localhost',
    })
    const recordSpy = vi.spyOn(dbQueryPerformance, 'record')

    try {
      await expect(storage.runMetricProbe()).resolves.toBe('ok')

      expect(recordSpy).toHaveBeenCalledWith(expect.any(Number), {
        name: 'MetricWithoutTenantAttribute',
        requestAborted: false,
        requestAbortedBeforeStart: false,
        requestAbortedAfterStart: false,
      })
      expect(recordSpy.mock.calls[0]?.[1]).not.toHaveProperty('tenantId')
    } finally {
      recordSpy.mockRestore()
    }
  })
})
