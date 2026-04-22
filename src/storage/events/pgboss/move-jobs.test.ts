import { vi } from 'vitest'

const { mockTransaction, mockGetQueue, mockError } = vi.hoisted(() => ({
  mockTransaction: vi.fn(),
  mockGetQueue: vi.fn(),
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

vi.mock('@internal/queue', () => ({
  PG_BOSS_SCHEMA: 'storage',
  Queue: {
    getInstance: () => ({
      getQueue: mockGetQueue,
    }),
  },
}))

vi.mock('../base-event', () => ({
  BaseEvent: class {},
}))

import { MoveJobs } from './move-jobs'

function makeJob(overrides?: Partial<Record<string, unknown>>) {
  return {
    data: {
      fromQueue: 'source-queue',
      toQueue: 'target-queue',
      deleteJobsFromOriginalQueue: false,
      sbReqId: 'sb-req-123',
    },
    ...overrides,
  }
}

function mockLockedTransaction(raw: ReturnType<typeof vi.fn>) {
  mockTransaction.mockImplementation(
    async (callback: (tnx: { raw: typeof raw }) => Promise<void>) => callback({ raw })
  )
}

describe('MoveJobs.handle', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('logs the missing target queue with sbReqId', async () => {
    const raw = vi.fn().mockResolvedValueOnce({
      rows: [{ pg_try_advisory_xact_lock: true }],
    })
    mockLockedTransaction(raw)
    mockGetQueue.mockResolvedValue(undefined)

    await expect(MoveJobs.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockError).toHaveBeenCalledWith(
      expect.anything(),
      '[PgBoss] Target queue target-queue does not exist',
      expect.objectContaining({
        type: 'pgboss',
        sbReqId: 'sb-req-123',
      })
    )
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
    mockGetQueue.mockResolvedValue({
      name: 'target-queue',
      policy: 'exactly_once',
    })

    await expect(MoveJobs.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockError).toHaveBeenCalledWith(
      expect.anything(),
      '[PgBoss] Error while copying jobs',
      expect.objectContaining({
        type: 'pgboss',
        error,
        sbReqId: 'sb-req-123',
      })
    )
  })
})
