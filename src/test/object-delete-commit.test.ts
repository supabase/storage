import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import {
  ObjectAdminDeleteAllBefore,
  ObjectAdminDeleteBatch,
  ObjectRemoved,
} from '../storage/events'
import { useStorage, withDeleteEnabled } from './utils/storage'

describe('object deletion commit boundary', () => {
  const helper = useStorage()
  class DeleteWorker extends ObjectAdminDeleteAllBefore {
    protected static override getOrCreateStorageBackend() {
      return helper.adapter
    }
  }
  let bucketId: string
  const name = 'key.txt'
  const content = 'keep these bytes'

  beforeEach(() => {
    bucketId = `delete-commit-${randomUUID()}`
  })

  afterEach(async () => {
    await helper.database.connection.query(`
      DROP TRIGGER IF EXISTS test_delete_commit_failure ON storage.objects;
      DROP FUNCTION IF EXISTS public.test_delete_commit_failure();
    `)
    vi.restoreAllMocks()
    const objects = await helper.database.listObjects(bucketId, 'id,name,version', 100)
    for (const object of objects) {
      await helper.adapter.deleteObject(
        helper.storage.location.getRootLocation(),
        key(object.name),
        object.version
      )
    }
    await withDeleteEnabled(helper.database.connection, async (trx) => {
      await trx.query('DELETE FROM storage.objects WHERE bucket_id=$1', [bucketId])
      await trx.query('DELETE FROM storage.buckets WHERE id=$1', [bucketId])
    })
  })

  const key = (objectName = name) =>
    helper.storage.location.getKeyLocation({
      tenantId: helper.database.tenantId,
      bucketId,
      objectName,
    })

  async function upload(body = content) {
    return helper.uploader.upload({
      bucketId,
      objectName: name,
      uploadType: 'standard',
      isUpsert: true,
      file: {
        body: Readable.from(body),
        mimeType: 'text/plain',
        cacheControl: 'no-cache',
        isTruncated: () => false,
      },
    })
  }

  async function read(version: string | null | undefined, objectName = name) {
    const object = await helper.adapter.getObject(
      helper.storage.location.getRootLocation(),
      key(objectName),
      version
    )
    const chunks: Buffer[] = []
    for await (const chunk of object.body as AsyncIterable<Buffer>) {
      chunks.push(Buffer.from(chunk))
    }
    return Buffer.concat(chunks).toString()
  }

  async function remove(mode: 'single' | 'bulk' | 'empty', versionId?: string) {
    if (mode === 'single') return helper.storage.from(bucketId).deleteObject(name, versionId)
    if (mode === 'bulk') {
      return helper.storage
        .from(bucketId)
        .deleteObjects([versionId ? { path: name, versionId } : name])
    }
    return DeleteWorker.handle({
      id: 'commit-boundary-test',
      name: DeleteWorker.queueName,
      expireInSeconds: 30,
      data: {
        tenant: helper.database.tenant(),
        bucketId,
        before: new Date(Date.now() + 60_000).toISOString(),
      },
    })
  }

  it.each([
    ['single', 'DISABLED'],
    ['bulk', 'DISABLED'],
    ['single', 'SUSPENDED'],
    ['bulk', 'SUSPENDED'],
    ['empty', 'DISABLED'],
    ['single', 'ENABLED'],
    ['bulk', 'ENABLED'],
  ] as const)('preserves metadata and bytes when %s/%s fails at COMMIT', async (mode, status) => {
    await helper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: status === 'DISABLED' ? 'DISABLED' : 'ENABLED',
    })
    if (status === 'SUSPENDED') {
      await helper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    }
    const preserved = status === 'ENABLED' ? await upload('retained version') : undefined
    const { obj } = await upload()
    await expect(read(obj.version)).resolves.toBe(content)
    const webhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const retry = vi.spyOn(ObjectAdminDeleteBatch, 'send').mockResolvedValue(undefined)

    await helper.database.connection.query(`
      CREATE FUNCTION public.test_delete_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF OLD.bucket_id = TG_ARGV[0] THEN
          RAISE EXCEPTION 'delete cleanup commit failure';
        END IF;
        RETURN NULL;
      END; $$;
      CREATE CONSTRAINT TRIGGER test_delete_commit_failure AFTER DELETE OR UPDATE ON storage.objects
      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
      EXECUTE FUNCTION public.test_delete_commit_failure('${bucketId}');
    `)

    await expect(remove(mode, status === 'ENABLED' ? obj.version : undefined)).rejects.toThrow(
      'database error, code: P0001'
    )
    await expect(
      helper.database.findObject(bucketId, name, 'id,version,is_delete_marker')
    ).resolves.toMatchObject({ version: obj.version, is_delete_marker: false })
    await expect(read(obj.version)).resolves.toBe(content)
    if (preserved) {
      await expect(read(preserved.obj.version)).resolves.toBe('retained version')
    }
    expect(webhook).not.toHaveBeenCalled()
    expect(retry).not.toHaveBeenCalled()
  })

  it.each(['single', 'bulk', 'empty'] as const)(
    '%s cleanup does not hold the metadata lock or delete a concurrent replacement',
    async (mode) => {
      await helper.database.createBucket({ id: bucketId, name: bucketId })
      const original = await upload()
      const started = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      const method = mode === 'single' ? 'deleteObject' : 'deleteObjectsDetailed'
      if (method === 'deleteObject') {
        const removeFile = helper.adapter.deleteObject.bind(helper.adapter)
        vi.spyOn(helper.adapter, method).mockImplementationOnce(async (...args) => {
          started.resolve()
          await release.promise
          return removeFile(...args)
        })
      } else {
        const removeFiles = helper.adapter.deleteObjectsDetailed.bind(helper.adapter)
        vi.spyOn(helper.adapter, method).mockImplementationOnce(async (...args) => {
          started.resolve()
          await release.promise
          return removeFiles(...args)
        })
      }
      const webhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
      const deletion = remove(mode)
      let replacement: Awaited<ReturnType<typeof upload>> | undefined
      try {
        await Promise.race([
          started.promise,
          deletion.then(() => {
            throw new Error('cleanup never started')
          }),
        ])
        // The database must already be committed while physical cleanup is paused.
        await expect(
          helper.database.findObject(bucketId, name, 'version', { dontErrorOnEmpty: true })
        ).resolves.toBeUndefined()
        replacement = await upload('replacement bytes')
        expect(replacement.obj.version).not.toBe(original.obj.version)
      } finally {
        release.resolve()
        await deletion
      }
      await expect(read(original.obj.version)).rejects.toThrow()
      await expect(read(replacement!.obj.version)).resolves.toBe('replacement bytes')
      expect(webhook).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ version: original.obj.version })
      )
    }
  )

  it.each(['single', 'bulk', 'empty'] as const)(
    '%s retries a failed backend deletion through the cleanup event',
    async (mode) => {
      await helper.database.createBucket({ id: bucketId, name: bucketId })
      const { obj } = await upload()
      const webhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
      const retry = vi.spyOn(ObjectAdminDeleteBatch, 'send')
      vi.spyOn(
        helper.adapter,
        mode === 'single' ? 'deleteObject' : 'deleteObjectsDetailed'
      ).mockRejectedValueOnce(new Error('transient backend failure'))

      await remove(mode)

      expect(retry).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ bucketId, objects: [{ name, version: obj.version }] })
      )
      await expect(
        helper.database.findObject(bucketId, name, 'version', { dontErrorOnEmpty: true })
      ).resolves.toBeUndefined()
      // With the queue disabled the batch event really deletes the file synchronously.
      await expect(read(obj.version)).rejects.toThrow()
      expect(webhook).toHaveBeenCalledOnce()
    }
  )

  it.each([
    { mode: 'single', retry: true },
    { mode: 'bulk', retry: true },
    { mode: 'empty', retry: true },
    { mode: 'empty', retry: false },
  ] as const)(
    '$mode preserves an unrelated legacy .info object (retry=$retry)',
    async ({ mode, retry }) => {
      await helper.database.createBucket({ id: bucketId, name: bucketId })
      for (const objectName of [name, name + '.info']) {
        await helper.database.connection.query(
          `INSERT INTO storage.objects (bucket_id, name, created_at) VALUES ($1, $2, $3)`,
          [bucketId, objectName, new Date(Date.now() + (objectName === name ? 0 : 120_000))]
        )
        await helper.adapter.uploadObject(
          helper.storage.location.getRootLocation(),
          key(objectName),
          undefined,
          Readable.from(content),
          'text/plain',
          'no-cache'
        )
      }
      await expect(read(undefined, name + '.info')).resolves.toBe(content)
      vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
      if (retry) {
        vi.spyOn(
          helper.adapter,
          mode === 'single' ? 'deleteObject' : 'deleteObjectsDetailed'
        ).mockRejectedValueOnce(new Error('transient backend failure'))
      }

      await remove(mode)

      await expect(read(undefined)).rejects.toThrow()
      await expect(read(undefined, name + '.info')).resolves.toBe(content)
    }
  )
})
