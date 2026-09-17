import { type Mock, vi } from 'vitest'

vi.mock('@storage/events/objects/backup-object', () => ({
  BackupObjectEvent: {
    batchSend: vi.fn(),
  },
}))

import { ObjectScanner } from '@storage/scanner/scanner'
import type { Storage } from '@storage/storage'

class TestObjectScanner extends ObjectScanner {
  async collectAllDbObjects(bucket: string) {
    const pages = []

    for await (const page of this.listAllDbObjects(bucket, {
      signal: new AbortController().signal,
    })) {
      pages.push(...page)
    }

    return pages
  }

  async collectAllS3Objects(prefix: string, before?: Date) {
    const pages = []

    for await (const page of this.listAllS3Objects(prefix, {
      before,
      signal: new AbortController().signal,
    })) {
      pages.push(...page)
    }

    return pages
  }
}

function makeScanner(params: { listS3Objects?: Mock; listDbObjects?: Mock }) {
  const storage = {
    backend: {
      list: params.listS3Objects ?? vi.fn(),
    },
    db: {
      listObjects: params.listDbObjects ?? vi.fn(),
    },
  } as unknown as Storage

  return new TestObjectScanner(storage)
}

describe('ObjectScanner pagination regressions', () => {
  test('continues within a key when object versions cross a database page boundary', async () => {
    const firstPage = Array.from({ length: 1000 }, (_, index) => ({
      name: 'deep-key',
      version: `version-${String(index).padStart(4, '0')}`,
      metadata: { size: index },
    }))
    const listDbObjects = vi
      .fn()
      .mockResolvedValueOnce(firstPage)
      .mockResolvedValueOnce([
        { name: 'deep-key', version: 'version-1000', metadata: { size: 1000 } },
        { name: 'sibling', version: 'version-0001', metadata: { size: 1 } },
      ])

    const scanner = makeScanner({ listDbObjects })
    const objects = await scanner.collectAllDbObjects('bucket')

    expect(objects).toHaveLength(1002)
    expect(objects.at(-2)).toEqual({ name: 'deep-key', version: 'version-1000', size: 1000 })
    expect(objects.at(-1)).toEqual({ name: 'sibling', version: 'version-0001', size: 1 })
    expect(listDbObjects.mock.calls[1]).toEqual([
      'bucket',
      'id,name,version,metadata',
      1000,
      undefined,
      'deep-key',
      'version-0999',
      { noncurrentVersions: 'include', deleteMarkers: 'exclude' },
    ])
  })

  test('retains a null version cursor after a legacy null-version row', async () => {
    const firstPage = [
      ...Array.from({ length: 999 }, (_, index) => ({
        name: `before-${String(index).padStart(4, '0')}`,
        version: 'version-1',
        metadata: { size: index },
      })),
      { name: 'legacy-key', version: null, metadata: { size: 1 } },
    ]
    const listDbObjects = vi
      .fn()
      .mockResolvedValueOnce(firstPage)
      .mockResolvedValueOnce([{ name: 'sibling', version: 'version-1', metadata: { size: 3 } }])

    const scanner = makeScanner({ listDbObjects })
    const objects = await scanner.collectAllDbObjects('bucket')

    expect(objects).toHaveLength(1001)
    expect(objects.at(-1)).toEqual({ name: 'sibling', version: 'version-1', size: 3 })
    expect(listDbObjects.mock.calls[1]).toEqual([
      'bucket',
      'id,name,version,metadata',
      1000,
      undefined,
      'legacy-key',
      null,
      { noncurrentVersions: 'include', deleteMarkers: 'exclude' },
    ])
  })

  test('continues scanning S3 pages after an empty filtered page when a continuation token remains', async () => {
    const listS3Objects = vi
      .fn()
      .mockResolvedValueOnce({
        keys: [
          { name: 'old-orphan-a/v0', size: 30 },
          { name: 'old-orphan-a/v0.info', size: 1 },
        ],
        nextToken: 'page-2',
      })
      .mockResolvedValueOnce({
        keys: [],
        nextToken: 'page-3',
      })
      .mockResolvedValueOnce({
        keys: [
          { name: 'old-orphan-b/v1', size: 10 },
          { name: 'old-orphan-b/v1.info', size: 1 },
          { name: 'old-orphan-c/v2', size: 20 },
        ],
        nextToken: undefined,
      })

    const scanner = makeScanner({ listS3Objects })

    await expect(scanner.collectAllS3Objects('tenant/bucket')).resolves.toEqual([
      { name: 'old-orphan-a/v0', size: 30 },
      { name: 'old-orphan-b/v1', size: 10 },
      { name: 'old-orphan-c/v2', size: 20 },
    ])

    expect(listS3Objects).toHaveBeenCalledTimes(3)
    expect(listS3Objects.mock.calls[0][1]).toMatchObject({
      prefix: 'tenant/bucket/',
    })
    expect(listS3Objects.mock.calls[1][1]).toMatchObject({
      prefix: 'tenant/bucket/',
      nextToken: 'page-2',
    })
    expect(listS3Objects.mock.calls[2][1]).toMatchObject({
      prefix: 'tenant/bucket/',
      nextToken: 'page-3',
    })
  })
})
