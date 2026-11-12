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
  test('isolates concurrent orphan scans even when they start in the same millisecond', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(1_234_567_890_123)

    const tables = new Map<string, Map<string, { key: string; size: number }>>()
    let insertArrivals = 0
    let releaseInserts!: () => void
    const insertGate = new Promise<void>((resolve) => {
      releaseInserts = resolve
    })
    let dropArrivals = 0
    let releaseDrops!: () => void
    const dropGate = new Promise<void>((resolve) => {
      releaseDrops = resolve
    })
    const db = {
      tenantId: 'tenant',
      createS3KeysTempTable: vi.fn(async (table: string) => {
        if (!tables.has(table)) tables.set(table, new Map())
      }),
      insertS3KeysIntoTempTable: vi.fn(
        async (table: string, rows: { key: string; size: number }[]) => {
          insertArrivals++
          if (insertArrivals === 2) releaseInserts()
          await insertGate
          for (const row of rows) tables.get(table)?.set(row.key, row)
        }
      ),
      listS3KeysFromTempTable: vi.fn(async (table: string, after: string, limit: number) =>
        [...(tables.get(table)?.values() ?? [])]
          .filter((row) => row.key > after)
          .sort((a, b) => a.key.localeCompare(b.key))
          .slice(0, limit)
      ),
      findS3KeysInTempTable: vi.fn(async (table: string, keys: string[]) =>
        keys.filter((key) => tables.get(table)?.has(key)).map((key) => ({ key }))
      ),
      listObjects: vi.fn(async (bucket: string) => [
        { name: bucket, version: 'v1', metadata: { size: 1 } },
      ]),
      findObjectVersions: vi.fn(async (bucket: string, keys: { name: string; version: string }[]) =>
        keys.filter((key) => key.name === bucket && key.version === 'v1')
      ),
      dropS3KeysTempTable: vi.fn(async (table: string) => {
        dropArrivals++
        if (dropArrivals === 2) releaseDrops()
        await dropGate
        tables.delete(table)
      }),
    }
    const storage = {
      db,
      backend: {
        list: vi.fn(async (_bucket: string, options: { prefix: string }) => {
          const bucket = options.prefix.split('/')[1]
          return { keys: [{ name: `${bucket}/v1`, size: 1 }] }
        }),
      },
    } as unknown as Storage
    const scanners = [new ObjectScanner(storage), new ObjectScanner(storage)]
    const collect = async (scanner: ObjectScanner, bucket: string) => {
      const orphans = []
      for await (const orphan of scanner.listOrphaned(bucket, {
        signal: new AbortController().signal,
      })) {
        orphans.push(orphan)
      }
      return orphans
    }

    const [first, second] = await Promise.all([
      collect(scanners[0], 'bucket-a'),
      collect(scanners[1], 'bucket-b'),
    ])

    expect(first).toEqual([])
    expect(second).toEqual([])
  })

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
