import { vi } from 'vitest'

const { mockBeginTransaction } = vi.hoisted(() => ({
  mockBeginTransaction: vi.fn(),
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
    error: vi.fn(),
    warning: vi.fn(),
  },
}))

import { logSchema } from '@internal/monitoring'
import { runUpgradeOnce } from './base'

function makeTransaction() {
  return {
    query: vi.fn(),
    commit: vi.fn(),
    rollback: vi.fn(),
  }
}

describe('runUpgradeOnce', () => {
  const upgrade = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('uses the pg transaction path and records the completed upgrade', async () => {
    const tx = makeTransaction()
    tx.query
      .mockResolvedValueOnce({ rows: [{ pg_try_advisory_xact_lock: true }] })
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] })
    mockBeginTransaction.mockResolvedValue(tx)
    upgrade.mockResolvedValue(undefined)

    await runUpgradeOnce('test-upgrade', upgrade)

    expect(upgrade).toHaveBeenCalledWith(tx)
    expect(tx.query).toHaveBeenLastCalledWith({
      text: expect.stringContaining('INSERT INTO event_upgrades'),
      values: ['test-upgrade'],
    })
    expect(tx.commit).toHaveBeenCalledTimes(1)
    expect(tx.rollback).not.toHaveBeenCalled()
  })

  it('commits without running the upgrade when the lock is held elsewhere', async () => {
    const tx = makeTransaction()
    tx.query.mockResolvedValueOnce({ rows: [{ pg_try_advisory_xact_lock: false }] })
    mockBeginTransaction.mockResolvedValue(tx)

    await runUpgradeOnce('test-upgrade', upgrade)

    expect(upgrade).not.toHaveBeenCalled()
    expect(tx.commit).toHaveBeenCalledTimes(1)
    expect(tx.rollback).not.toHaveBeenCalled()
  })

  it('commits without running the upgrade when the event is already recorded', async () => {
    const tx = makeTransaction()
    tx.query
      .mockResolvedValueOnce({ rows: [{ pg_try_advisory_xact_lock: true }] })
      .mockResolvedValueOnce({ rows: [{ event_id: 'test-upgrade' }] })
    mockBeginTransaction.mockResolvedValue(tx)

    await runUpgradeOnce('test-upgrade', upgrade)

    expect(upgrade).not.toHaveBeenCalled()
    expect(tx.commit).toHaveBeenCalledTimes(1)
    expect(tx.rollback).not.toHaveBeenCalled()
  })

  it('rolls back when the upgrade callback fails', async () => {
    const error = new Error('upgrade failed')
    const tx = makeTransaction()
    tx.query
      .mockResolvedValueOnce({ rows: [{ pg_try_advisory_xact_lock: true }] })
      .mockResolvedValueOnce({ rows: [] })
    mockBeginTransaction.mockResolvedValue(tx)
    upgrade.mockRejectedValue(error)

    await expect(runUpgradeOnce('test-upgrade', upgrade)).rejects.toThrow(error)

    expect(tx.commit).not.toHaveBeenCalled()
    expect(tx.rollback).toHaveBeenCalledTimes(1)
  })

  it('preserves upgrade errors when rollback fails', async () => {
    const error = new Error('upgrade failed')
    const rollbackError = new Error('rollback failed')
    const tx = makeTransaction()
    tx.query
      .mockResolvedValueOnce({ rows: [{ pg_try_advisory_xact_lock: true }] })
      .mockResolvedValueOnce({ rows: [] })
    tx.rollback.mockRejectedValue(rollbackError)
    mockBeginTransaction.mockResolvedValue(tx)
    upgrade.mockRejectedValue(error)

    await expect(runUpgradeOnce('test-upgrade', upgrade)).rejects.toBe(error)

    expect(tx.commit).not.toHaveBeenCalled()
    expect(tx.rollback).toHaveBeenCalledTimes(1)
    expect(logSchema.warning).toHaveBeenCalledWith(
      expect.anything(),
      '[Upgrade] Failed to rollback transaction',
      expect.objectContaining({
        type: 'upgradeEvent',
        error: rollbackError,
      })
    )
  })
})
