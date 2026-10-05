import { vi } from 'vitest'
import { TenantMigrationStatus } from '../tenant'
import { highestLocalMigrationName } from './files'
import { readTenantMigrationVersion } from './migrate'
import { cacheTenantMigration, getCachedTenantMigration, observeTenantMigration } from './observed'

vi.mock('./migrate', () => ({ readTenantMigrationVersion: vi.fn() }))
vi.mock('../../monitoring', () => ({ logger: {}, logSchema: { warning: vi.fn() } }))

const read = vi.mocked(readTenantMigrationVersion)

function tenant(version?: string): Parameters<typeof observeTenantMigration>[1] {
  return {
    databaseUrl: 'postgres://tenant',
    migrationVersion: version as Parameters<typeof observeTenantMigration>[1]['migrationVersion'],
  }
}

beforeEach(() => read.mockReset())
afterEach(() => vi.useRealTimers())

it.each([
  'read',
  'run',
])('rechecks an ahead %s after 30 s and coalesces the refresh', async (source) => {
  vi.useFakeTimers({ toFake: ['performance'] })
  const config = tenant('objects-key-version-index')
  const refreshed = Promise.withResolvers<string>()
  if (source === 'read') {
    read.mockResolvedValueOnce('future-migration')
    await observeTenantMigration('tenant', config)
  } else {
    cacheTenantMigration(config, 'future-migration')
  }
  read.mockReturnValueOnce(refreshed.promise)
  const cached = config.observedMigration
  await expect(cached).resolves.toBe(highestLocalMigrationName())
  vi.advanceTimersByTime(29_999)
  expect(observeTenantMigration('tenant', config)).toBe(cached)
  vi.advanceTimersByTime(1)
  const refresh = observeTenantMigration('tenant', config)
  expect(refresh).not.toBe(cached)
  expect(observeTenantMigration('tenant', config)).toBe(refresh)
  refreshed.resolve('objects-key-version-index')
  await expect(refresh).resolves.toBe('objects-key-version-index')
  expect(config.observedMigrationName).toBe('objects-key-version-index')
  expect(config.observedMigrationExpiresAt).toBeUndefined()
  expect(read).toHaveBeenCalledTimes(source === 'read' ? 2 : 1)
})

it('keeps an ahead observation under a completed unrecognized row with the config', async () => {
  vi.useFakeTimers({ toFake: ['performance'] })
  const config = tenant('future-migration')
  config.migrationStatus = TenantMigrationStatus.COMPLETED
  read.mockResolvedValue('future-migration')
  const observed = observeTenantMigration('tenant', config)
  await expect(observed).resolves.toBe(highestLocalMigrationName())
  expect(config.observedMigrationExpiresAt).toBeUndefined()
  vi.advanceTimersByTime(30_000)
  expect(observeTenantMigration('tenant', config)).toBe(observed)
  expect(read).toHaveBeenCalledOnce()
})

it.each([
  ['objects-key-version-index', 'FAILED', 'objects-key-version-index'],
  ['future-migration', 'COMPLETED', highestLocalMigrationName()],
  ['future-migration', 'FAILED', undefined],
  ['future-migration', undefined, undefined],
  [undefined, 'COMPLETED', undefined],
])('retries a failed observation with %s / %s after 5 s without evicting config', async (version, status, fallback) => {
  vi.useFakeTimers({ toFake: ['performance'] })
  const config = tenant(version)
  config.migrationStatus = status as typeof config.migrationStatus
  const failed = Promise.withResolvers<string>()
  read.mockReturnValueOnce(failed.promise).mockResolvedValue('initialmigration')
  const first = observeTenantMigration('tenant', config)
  expect(observeTenantMigration('tenant', config)).toBe(first)
  failed.reject(new Error('ledger unavailable'))
  if (fallback) {
    await expect(first).resolves.toBe(fallback)
  } else {
    await expect(first).rejects.toMatchObject({ code: 'DatabaseError', httpStatusCode: 503 })
  }
  vi.advanceTimersByTime(4_999)
  expect(observeTenantMigration('tenant', config)).toBe(first)
  expect(read).toHaveBeenCalledOnce()
  vi.advanceTimersByTime(1)
  expect(getCachedTenantMigration(config)).toBeUndefined()
  expect(config.migrationVersion).toBe(version)
  expect(config.migrationStatus).toBe(status)
  await expect(observeTenantMigration('tenant', config)).resolves.toBe('initialmigration')
  await expect(observeTenantMigration('tenant', config)).resolves.toBe('initialmigration')
  expect(read).toHaveBeenCalledTimes(2)
})

it.each([
  'success',
  'failure',
])('keeps a replacement observation when the old read ends in %s', async (outcome) => {
  vi.useFakeTimers({ toFake: ['performance'] })
  const config = tenant('objects-key-version-index')
  const ledger = Promise.withResolvers<string>()
  read.mockReturnValueOnce(ledger.promise)
  const first = observeTenantMigration('tenant', config)
  cacheTenantMigration(config, 'initialmigration')
  const replacement = config.observedMigration
  if (outcome === 'success') ledger.resolve('future-migration')
  else ledger.reject(new Error('ledger unavailable'))
  await expect(first).resolves.toBe(
    outcome === 'success' ? highestLocalMigrationName() : 'objects-key-version-index'
  )
  vi.advanceTimersByTime(30_000)
  expect(getCachedTenantMigration(config)).toBe(replacement)
  expect(config.observedMigrationName).toBe('initialmigration')
  expect(config.observedMigrationExpiresAt).toBeUndefined()
})
