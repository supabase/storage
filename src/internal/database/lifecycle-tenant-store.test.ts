import type { DatabaseExecutor } from './connection'
import { LifecycleTenantStorePg } from './lifecycle-tenant-store'

describe('LifecycleTenantStorePg.wakeTenants', () => {
  function createStore() {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 })
    const store = new LifecycleTenantStorePg({ query } as unknown as DatabaseExecutor)
    return { query, store }
  }

  it('short-circuits empty input without querying', async () => {
    const { query, store } = createStore()

    await expect(
      store.wakeTenants({ tenantIds: [], storageVersioningEnabled: false })
    ).resolves.toEqual({
      wokenTenantIds: [],
      disabledTenantIds: [],
    })
    expect(query).not.toHaveBeenCalled()
  })

  it('partitions woken vs disabled rows and forwards abort signal', async () => {
    const { query, store } = createStore()
    const signal = new AbortController().signal
    query.mockResolvedValueOnce({
      rows: [
        { tenantId: 'tenant-a', woken: true },
        { tenantId: 'tenant-b', woken: false },
      ],
      rowCount: 2,
    })

    await expect(
      store.wakeTenants({
        tenantIds: ['tenant-b', 'tenant-a', 'missing'],
        storageVersioningEnabled: false,
        signal,
      })
    ).resolves.toEqual({
      wokenTenantIds: ['tenant-a'],
      disabledTenantIds: ['tenant-b'],
    })

    expect(query).toHaveBeenCalledWith(
      expect.objectContaining({
        values: [['tenant-b', 'tenant-a', 'missing'], false],
      }),
      { signal }
    )
  })
})
