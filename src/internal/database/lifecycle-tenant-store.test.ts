import type { DatabaseTransactionalExecutor } from './connection'
import { LifecycleTenantStorePg } from './lifecycle-tenant-store'

function fixture(rows: Record<string, unknown>[] = [], rowCount = rows.length) {
  const database = {
    query: vi.fn().mockResolvedValue({ rows, rowCount }),
    beginTransaction: vi.fn(),
  } as unknown as DatabaseTransactionalExecutor
  return { database, store: new LifecycleTenantStorePg(database) }
}

describe('LifecycleTenantStorePg', () => {
  test('wake preserves an earlier dispatch hint with a database-clock timestamp', async () => {
    const { database, store } = fixture()
    const signal = new AbortController().signal
    await store.wakeTenant('tenant-a', signal)

    expect(database.query).toHaveBeenCalledWith(
      {
        text: expect.stringMatching(/VALUES \([\s\S]*clock_timestamp\(\)[\s\S]*ON CONFLICT/),
        values: ['tenant-a'],
      },
      { signal }
    )
    expect((vi.mocked(database.query).mock.calls[0]?.[0] as { text: string }).text).not.toContain(
      'updated_at'
    )
  })

  test('claims consume the due hint', async () => {
    const { database, store } = fixture([
      {
        tenant_id: 'tenant-a',
        claim_id: '49f3e225-7455-4b2e-bf49-2ca7abc80b49',
      },
    ])

    await expect(store.claimDueTenants(10, 60_000)).resolves.toEqual([
      {
        tenantId: 'tenant-a',
        claimId: '49f3e225-7455-4b2e-bf49-2ca7abc80b49',
      },
    ])

    const statement = vi.mocked(database.query).mock.calls[0]?.[0]
    expect(statement).toMatchObject({
      text: expect.stringContaining("next_dispatch_at = 'infinity'::timestamptz"),
      values: [10, 60_000],
    })
    expect((statement as { text: string }).text).not.toContain('updated_at')
  })

  test('completion and failure preserve earlier concurrent wakes with LEAST', async () => {
    const { database, store } = fixture([], 1)
    const claimId = '49f3e225-7455-4b2e-bf49-2ca7abc80b49'

    await expect(
      store.completeTenantDispatch('tenant-a', claimId, '2026-08-16T00:00:00.000Z')
    ).resolves.toBe(true)
    await expect(
      store.failTenantDispatch('tenant-a', claimId, {
        code: 'timeout',
      })
    ).resolves.toBe(true)

    for (const [statement] of vi.mocked(database.query).mock.calls) {
      expect(statement).toMatchObject({ text: expect.stringContaining('LEAST(next_dispatch_at') })
      expect((statement as { text: string }).text).not.toContain('updated_at')
    }
  })
})
