import { vi } from 'vitest'

const { mockBeginTransaction, mockGetQueues, mockError } = vi.hoisted(() => ({
  mockBeginTransaction: vi.fn(),
  mockGetQueues: vi.fn(),
  mockError: vi.fn(),
}))

vi.mock('@internal/database', () => ({
  multitenantPgExecutor: {
    beginTransaction: mockBeginTransaction,
  },
}))

vi.mock('@internal/monitoring', () => ({
  logger: {},
  logSchema: {
    info: vi.fn(),
    error: mockError,
    warning: vi.fn(),
  },
}))

vi.mock('@internal/queue', async () => {
  const { SYSTEM_TENANT, SYSTEM_TENANT_REF } = await vi.importActual<
    typeof import('@internal/queue/constants')
  >('@internal/queue/constants')

  return {
    PG_BOSS_SCHEMA: 'storage',
    SYSTEM_TENANT,
    SYSTEM_TENANT_REF,
    Queue: {
      getInstance: () => ({
        getQueues: mockGetQueues,
      }),
    },
  }
})

vi.mock('../base-event', () => ({
  BaseEvent: class {},
}))

import { SYSTEM_TENANT, SYSTEM_TENANT_REF } from '@internal/queue'
import { UpgradePgBossV10 } from './upgrade-v10'

function makeJob(overrides?: Partial<Record<string, unknown>>) {
  return {
    data: {
      sbReqId: 'sb-req-123',
      tenant: SYSTEM_TENANT,
    },
    ...overrides,
  }
}

function mockLockedTransaction(query: ReturnType<typeof vi.fn>) {
  mockBeginTransaction.mockResolvedValue({
    query,
    commit: vi.fn(),
    rollback: vi.fn(),
  })
}

describe('UpgradePgBossV10.handle', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('logs copy failures with sbReqId', async () => {
    const error = new Error('copy failed')
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [{ locked: true }],
      })
      .mockRejectedValueOnce(error)
    mockLockedTransaction(query)
    mockGetQueues.mockResolvedValue([
      {
        name: 'queue-a',
        policy: 'exactly_once',
      },
    ])

    await expect(UpgradePgBossV10.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockError).toHaveBeenCalledWith(
      expect.anything(),
      '[PgBoss] Error while copying jobs',
      expect.objectContaining({
        type: 'pgboss',
        error,
        project: SYSTEM_TENANT_REF,
        sbReqId: 'sb-req-123',
      })
    )
  })
})
