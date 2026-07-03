import { vi } from 'vitest'

const { mockBeginTransaction, mockError, mockGetQueue } = vi.hoisted(() => ({
  mockBeginTransaction: vi.fn(),
  mockError: vi.fn(),
  mockGetQueue: vi.fn(),
}))

vi.mock('../../../config', () => ({
  getConfig: () => ({}),
}))

vi.mock('@internal/database', () => ({
  multitenantPgExecutor: {
    beginTransaction: mockBeginTransaction,
  },
  PgTransaction: class {},
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
  PG_BOSS_SCHEMA: 'pgboss_v12',
  SYSTEM_TENANT_REF: 'system-tenant',
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
import { UpgradePgBossV12 } from './upgrade-v12'

function makeTransaction() {
  return {
    query: vi.fn(),
    commit: vi.fn(),
    rollback: vi.fn(),
  }
}

function makeMoveJob() {
  return {
    data: {
      fromQueue: 'source-queue',
      toQueue: 'target-queue',
      deleteJobsFromOriginalQueue: false,
      sbReqId: 'sb-req-123',
      tenant: {
        ref: 'tenant-a',
      },
    },
  }
}

function makeUpgradeJob() {
  return {
    data: {
      sbReqId: 'sb-req-456',
      tenant: {
        ref: 'tenant-a',
      },
    },
  }
}

describe('pg-boss maintenance pg branches', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('moves jobs through the pg transaction branch', async () => {
    const tx = makeTransaction()
    tx.query.mockResolvedValueOnce({ rows: [{ locked: true }] }).mockResolvedValueOnce({ rows: [] })
    mockBeginTransaction.mockResolvedValue(tx)
    mockGetQueue.mockResolvedValue({
      name: 'target-queue',
      policy: 'exclusive',
    })

    await MoveJobs.handle(makeMoveJob() as never)

    expect(tx.query).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        text: expect.stringContaining('INSERT INTO pgboss_v12.job'),
        values: ['target-queue', 'exclusive', 'source-queue'],
      })
    )
    expect(tx.commit).toHaveBeenCalledTimes(1)
    expect(tx.rollback).not.toHaveBeenCalled()
  })

  it('copies pending jobs from the pgboss_v10 schema through the pg transaction branch', async () => {
    const tx = makeTransaction()
    tx.query
      .mockResolvedValueOnce({ rows: [{ locked: true }] })
      .mockResolvedValueOnce({ rows: [{ exists: true }] })
      .mockResolvedValueOnce({ rows: [] })
    mockBeginTransaction.mockResolvedValue(tx)

    await UpgradePgBossV12.handle(makeUpgradeJob() as never)

    const copySql = tx.query.mock.calls[2][0] as string
    expect(copySql).toContain('INSERT INTO pgboss_v12.job')
    expect(copySql).toContain('FROM pgboss_v10.job')
    expect(tx.commit).toHaveBeenCalledTimes(1)
    expect(tx.rollback).not.toHaveBeenCalled()
  })

  it('skips the copy when the pgboss_v10 schema does not exist', async () => {
    const tx = makeTransaction()
    tx.query
      .mockResolvedValueOnce({ rows: [{ locked: true }] })
      .mockResolvedValueOnce({ rows: [{ exists: false }] })
    mockBeginTransaction.mockResolvedValue(tx)

    await UpgradePgBossV12.handle(makeUpgradeJob() as never)

    expect(tx.query).toHaveBeenCalledTimes(2)
    expect(tx.commit).toHaveBeenCalledTimes(1)
    expect(tx.rollback).not.toHaveBeenCalled()
  })
})
