import { vi } from 'vitest'

const { mockTransaction, mockGetQueues, mockError } = vi.hoisted(() => ({
  mockTransaction: vi.fn(),
  mockGetQueues: vi.fn(),
  mockError: vi.fn(),
}))

vi.mock('@internal/database', () => ({
  multitenantKnex: {
    transaction: mockTransaction,
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

function mockLockedTransaction(raw: ReturnType<typeof vi.fn>) {
  mockTransaction.mockImplementation(
    async (callback: (tnx: { raw: typeof raw }) => Promise<void>) => callback({ raw })
  )
}

describe('UpgradePgBossV10.handle', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('logs copy failures with sbReqId', async () => {
    const error = new Error('copy failed')
    const raw = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [{ pg_try_advisory_xact_lock: true }],
      })
      .mockRejectedValueOnce(error)
    mockLockedTransaction(raw)
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
