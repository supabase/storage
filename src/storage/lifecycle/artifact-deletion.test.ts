import { lifecycleArtifactOutcomes } from '@internal/monitoring/metrics'
import * as config from '../../config'
import type { Database } from '../database'
import type { Storage } from '../storage'
import { deleteLifecycleArtifacts } from './artifact-deletion'

vi.mock('@internal/monitoring/metrics', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@internal/monitoring/metrics')>()),
  lifecycleArtifactOutcomes: { add: vi.fn() },
}))

type Result = { key: string; outcome: string; error?: { message?: string; code?: string } }

function setup(storageBackendType: 's3' | 'file', results?: (keys: string[]) => Result[]) {
  vi.spyOn(config, 'getConfig').mockReturnValue({ ...config.getConfig(), storageBackendType })
  const backend = {
    deleteObjectsDetailed: vi
      .fn()
      .mockImplementation((_bucket: string, keys: string[]) =>
        results ? results(keys) : keys.map((key) => ({ key, outcome: 'DELETED' }))
      ),
  }
  const database = { tenantId: 'tenant', resolveLifecycleAbandonedArtifacts: vi.fn() }
  const storage = {
    backend,
    location: {
      getRootLocation: () => 'root',
      getKeyLocation: ({ tenantId, bucketId, objectName }: Record<string, string>) =>
        `${tenantId}/${bucketId}/${objectName}`,
    },
  } as unknown as Storage
  const run = (
    artifacts: Array<{ bucketId: string; key: string }>,
    signal = new AbortController().signal
  ) =>
    deleteLifecycleArtifacts({
      storage,
      database: database as unknown as Database,
      artifacts,
      signal,
    })
  return { backend, database, run }
}

const artifact = (key: string) => ({ bucketId: 'one', key })
const keyOfBytes = (bytes: number) => 'a'.repeat(bytes - 'tenant/one/'.length)

describe('deleteLifecycleArtifacts', () => {
  it('resolves S3 keys beyond the key limit as absent without sending them', async () => {
    const { backend, database, run } = setup('s3')
    const exact = artifact(keyOfBytes(1024))
    const over = artifact(keyOfBytes(1025))
    const short = artifact('a/v1')

    await expect(run([over, exact, short])).resolves.toMatchObject({ absent: 1, deleted: 2 })

    expect(backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith(
      'root',
      [`tenant/one/${exact.key}`, 'tenant/one/a/v1'],
      { signal: expect.any(AbortSignal) }
    )
    expect(database.resolveLifecycleAbandonedArtifacts).toHaveBeenCalledExactlyOnceWith(
      [over, exact, short],
      []
    )
    expect(lifecycleArtifactOutcomes.add).toHaveBeenCalledWith(1, { outcome: 'ABSENT' })
    expect(lifecycleArtifactOutcomes.add).toHaveBeenCalledWith(2, { outcome: 'DELETED' })
  })

  it('measures the S3 key limit in bytes, not characters', async () => {
    const { backend, run } = setup('s3')
    const multibyte = artifact('€'.repeat(342))

    expect(multibyte.key.length + 'tenant/one/'.length).toBeLessThan(1024)
    await expect(run([multibyte])).resolves.toMatchObject({ absent: 1, deleted: 0 })
    expect(backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it('does not call the backend when every key is over the limit', async () => {
    const { backend, run } = setup('s3')

    await expect(run([artifact(keyOfBytes(1025))])).resolves.toEqual({
      absent: 1,
      deleted: 0,
      failed: 0,
      error: undefined,
    })
    expect(backend.deleteObjectsDetailed).not.toHaveBeenCalled()
  })

  it('sends a file backend key beyond the S3 limit', async () => {
    const { backend, run } = setup('file')
    const long = artifact(keyOfBytes(1025))
    const { signal } = new AbortController()

    await expect(run([long], signal)).resolves.toMatchObject({ absent: 0, deleted: 1 })
    expect(backend.deleteObjectsDetailed).toHaveBeenCalledExactlyOnceWith(
      'root',
      [`tenant/one/${long.key}`],
      { signal }
    )
  })

  it.each([
    { label: 'swapped', results: (keys: string[]) => [...keys].reverse(), deleted: 0 },
    { label: 'truncated', results: (keys: string[]) => keys.slice(0, 1), deleted: 1 },
    { label: 'mismatched', results: (keys: string[]) => [keys[0], 'other'], deleted: 1 },
  ])('counts $label results as failed by key position', async ({ results, deleted }) => {
    const { database, run } = setup('file', (keys) =>
      results(keys).map((key) => ({ key, outcome: 'DELETED' }))
    )
    const [first, second] = [artifact('a'), artifact('b')]

    await expect(run([first, second])).resolves.toMatchObject({ deleted, failed: 2 - deleted })
    expect(database.resolveLifecycleAbandonedArtifacts).toHaveBeenCalledExactlyOnceWith(
      deleted ? [first] : [],
      deleted ? [second] : [first, second]
    )
  })

  it.each(['FAILED', 'UNKNOWN'])('counts %s as failed with a FAILED outcome', async (outcome) => {
    const { run } = setup('file', (keys) => keys.map((key) => ({ key, outcome })))

    await expect(run([artifact('a')])).resolves.toMatchObject({ failed: 1, deleted: 0 })
    expect(lifecycleArtifactOutcomes.add).toHaveBeenCalledExactlyOnceWith(1, { outcome: 'FAILED' })
  })

  it.each([
    { error: { message: 'denied', code: 'AccessDenied' }, expected: 'denied' },
    { error: { message: '', code: 'AccessDenied' }, expected: 'AccessDenied' },
  ])('reports the first failed result error $expected', async ({ error, expected }) => {
    const { run } = setup('file', (keys) =>
      keys.map((key, index) => ({
        key,
        outcome: 'FAILED',
        error: index === 0 ? error : { message: 'second' },
      }))
    )

    await expect(run([artifact('a'), artifact('b')])).resolves.toEqual({
      absent: 0,
      deleted: 0,
      failed: 2,
      error: expected,
    })
  })
})
