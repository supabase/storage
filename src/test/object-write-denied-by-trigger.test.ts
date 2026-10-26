import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import type { FastifyInstance } from 'fastify'
import buildApp from '../app'
import { getConfig } from '../config'
import { StoragePgDB } from '../storage/database'
import { ObjectAdminDelete, ObjectCreatedPostEvent, ObjectCreatedPutEvent } from '../storage/events'
import { Uploader } from '../storage/uploader'
import { useStorage, withDeleteEnabled } from './utils/storage'

describe('object writes denied by a tenant trigger', () => {
  const store = useStorage()
  const { serviceKeyAsync, storageS3Bucket, storageBackendType, tenantId } = getConfig()
  let app: FastifyInstance
  let bucketId: string
  let authorization: string
  let versions: string[]
  let triggers: Array<{ name: string; fn: string }>

  beforeEach(async () => {
    bucketId = `trigger-deny-${randomUUID()}`
    versions = []
    triggers = []
    authorization = `Bearer ${await serviceKeyAsync}`
    await store.database.createBucket({ id: bucketId, name: bucketId, public: false })
    vi.spyOn(ObjectCreatedPostEvent, 'sendWebhook').mockResolvedValue(undefined)
    vi.spyOn(ObjectCreatedPutEvent, 'sendWebhook').mockResolvedValue(undefined)
    // Run the queued cleanup inline so the assertions can look at the bytes.
    vi.spyOn(ObjectAdminDelete.prototype, 'send').mockImplementation(async function (
      this: ObjectAdminDelete
    ) {
      await ObjectAdminDelete.handle({
        id: '__sync',
        name: ObjectAdminDelete.queueName,
        expireInSeconds: 0,
        data: this.payload,
      })
    })
    // Record every version whose bytes reach the backend.
    const completeUpload = Uploader.prototype.completeUpload
    vi.spyOn(Uploader.prototype, 'completeUpload').mockImplementation(async function (
      this: Uploader,
      options
    ) {
      versions.push(options.version)
      return completeUpload.call(this, options)
    })
    app = buildApp()
    await app.ready()
  })

  afterEach(async () => {
    await app.close()
    for (const { name, fn } of triggers) {
      await store.database.connection.query(`DROP TRIGGER IF EXISTS ${name} ON storage.objects`)
      await store.database.connection.query(`DROP FUNCTION IF EXISTS ${fn}()`)
    }
    await withDeleteEnabled(store.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
    })
  })

  /**
   * Installs a trigger scoped to this test's bucket whose body decides, from
   * `condition`, whether to return NULL (deny) or the row (allow).
   */
  async function installTrigger(event: string, condition: string) {
    const suffix = randomUUID().replaceAll('-', '')
    const name = `deny_trg_${suffix}`
    const fn = `deny_fn_${suffix}`
    await store.database.connection.query(`
      CREATE FUNCTION ${fn}() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.bucket_id = '${bucketId}' AND (${condition}) THEN
          RETURN NULL;
        END IF;
        RETURN NEW;
      END
      $$
    `)
    await store.database.connection.query(`
      CREATE TRIGGER ${name} BEFORE ${event} ON storage.objects
      FOR EACH ROW EXECUTE FUNCTION ${fn}()
    `)
    triggers.push({ name, fn })
  }

  async function rowsFor(name: string) {
    const result = await store.database.connection.query<{
      version: string
      archived_at: string | null
    }>('SELECT version, archived_at FROM storage.objects WHERE bucket_id = $1 AND name = $2', [
      bucketId,
      name,
    ])
    return result.rows
  }

  async function expectNoBytes(name: string, version: string) {
    const key = store.storage.location.getKeyLocation({ tenantId, bucketId, objectName: name })
    await expect(store.adapter.headObject(storageS3Bucket, key, version)).rejects.toMatchObject(
      storageBackendType === 'file' ? { code: 'ENOENT' } : { httpStatusCode: 404 }
    )
  }

  async function expectBytes(name: string, version: string) {
    const key = store.storage.location.getKeyLocation({ tenantId, bucketId, objectName: name })
    await expect(store.adapter.headObject(storageS3Bucket, key, version)).resolves.toBeDefined()
  }

  function upload(name: string, content: string, options: { upsert?: boolean } = {}) {
    return app.inject({
      method: 'POST',
      url: `/object/${bucketId}/${name}`,
      headers: {
        authorization,
        'content-type': 'text/plain',
        ...(options.upsert ? { 'x-upsert': 'true' } : {}),
      },
      payload: content,
    })
  }

  const rejected = {
    statusCode: '403',
    error: 'Unauthorized',
    code: 'AccessDenied',
    message: 'Object write was rejected by a database trigger',
  }

  describe('unexpected failures after the bytes are stored', () => {
    it('removes the uploaded bytes when the write returns no row', async () => {
      vi.spyOn(StoragePgDB.prototype, 'upsertObject').mockResolvedValue(undefined as never)
      const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send')
      const name = `no-row-${randomUUID()}.txt`

      await expect(
        store.uploader.upload({
          bucketId,
          objectName: name,
          isUpsert: false,
          uploadType: 'standard',
          file: {
            body: Readable.from(['hello']),
            mimeType: 'text/plain',
            cacheControl: 'no-cache',
            isTruncated: () => false,
          },
        })
      ).rejects.toMatchObject({ code: 'InternalError', message: 'Object write returned no row' })

      expect(versions).toHaveLength(1)
      expect(deleteSpy).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ bucketId, name, version: versions[0] })
      )
      expect(await rowsFor(name)).toEqual([])
      await expectNoBytes(name, versions[0])
    })
  })

  describe('POST upload', () => {
    it('rejects a new object denied by a BEFORE INSERT trigger before uploading any bytes', async () => {
      await installTrigger('INSERT', 'true')
      const name = `insert-${randomUUID()}.txt`

      const response = await upload(name, 'hello')

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(name)).toEqual([])
      // The pre-upload probe sees the denial, so no bytes are ever stored.
      expect(versions).toEqual([])
    })

    it('removes the bytes when the trigger only denies the final write', async () => {
      // The probe writes with version '1'; the real write carries a UUID.
      await installTrigger('INSERT', `NEW.version <> '1'`)
      const name = `late-deny-${randomUUID()}.txt`

      const response = await upload(name, 'hello')

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(name)).toEqual([])
      expect(versions).toHaveLength(1)
      await expectNoBytes(name, versions[0])
    })
  })

  describe('upsert', () => {
    it('rejects a new object denied by a BEFORE INSERT trigger', async () => {
      await installTrigger('INSERT', 'true')
      const name = `upsert-insert-${randomUUID()}.txt`

      const response = await upload(name, 'hello', { upsert: true })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(name)).toEqual([])
      expect(versions).toEqual([])
    })

    it('rejects an overwrite denied by a BEFORE UPDATE trigger and keeps the original', async () => {
      const name = `upsert-update-${randomUUID()}.txt`
      const first = await upload(name, 'original')
      expect(first.statusCode).toBe(200)
      const [originalVersion] = versions
      await installTrigger('UPDATE', 'true')

      const response = await upload(name, 'replacement', { upsert: true })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(versions).toHaveLength(1)
      expect((await rowsFor(name)).map((row) => row.version)).toEqual([originalVersion])
      await expectBytes(name, originalVersion)
    })

    it('removes the new bytes when the trigger only denies the final overwrite', async () => {
      const name = `upsert-late-${randomUUID()}.txt`
      const first = await upload(name, 'original')
      expect(first.statusCode).toBe(200)
      const [originalVersion] = versions
      await installTrigger('UPDATE', `NEW.version <> '1' AND NEW.version <> '${originalVersion}'`)

      const response = await upload(name, 'replacement', { upsert: true })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(versions).toHaveLength(2)
      expect((await rowsFor(name)).map((row) => row.version)).toEqual([originalVersion])
      await expectBytes(name, originalVersion)
      await expectNoBytes(name, versions[1])
    })
  })

  describe('copy and move', () => {
    async function seedSource(name: string) {
      const response = await upload(name, 'source')
      expect(response.statusCode).toBe(200)
      return versions[0]
    }

    function copy(source: string, destination: string) {
      return app.inject({
        method: 'POST',
        url: '/object/copy',
        headers: { authorization },
        payload: { bucketId, sourceKey: source, destinationKey: destination },
      })
    }

    it('rejects a copy denied by the permission probe before copying any bytes', async () => {
      const source = `copy-src-${randomUUID()}.txt`
      const destination = `copy-dst-${randomUUID()}.txt`
      const sourceVersion = await seedSource(source)
      await installTrigger('INSERT', `NEW.name = '${destination}'`)
      const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send')

      const response = await copy(source, destination)

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(destination)).toEqual([])
      expect(deleteSpy).not.toHaveBeenCalled()
      await expectBytes(source, sourceVersion)
    })

    it('removes the copied bytes when the trigger only denies the final write', async () => {
      const source = `copy-src-${randomUUID()}.txt`
      const destination = `copy-dst-${randomUUID()}.txt`
      const sourceVersion = await seedSource(source)
      await installTrigger('INSERT', `NEW.name = '${destination}' AND NEW.version <> '1'`)
      const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send')

      const response = await copy(source, destination)

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(destination)).toEqual([])
      expect(deleteSpy).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ bucketId, name: destination })
      )
      const { version } = deleteSpy.mock.calls[0][0] as { version?: string }
      expect(version).not.toBe(sourceVersion)
      await expectNoBytes(destination, version as string)
      await expectBytes(source, sourceVersion)
    })

    it('rejects a versioned move denied by the permission probe and keeps the source', async () => {
      const source = `move-src-${randomUUID()}.txt`
      const destination = `move-dst-${randomUUID()}.txt`
      await store.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
      const sourceVersion = await seedSource(source)
      await installTrigger('INSERT', `NEW.name = '${destination}'`)
      const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send')

      const response = await app.inject({
        method: 'POST',
        url: '/object/move',
        headers: { authorization },
        payload: { bucketId, sourceKey: source, destinationKey: destination },
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(destination)).toEqual([])
      expect((await rowsFor(source)).map((row) => row.version)).toEqual([sourceVersion])
      expect(deleteSpy).not.toHaveBeenCalled()
      await expectBytes(source, sourceVersion)
    })
  })

  describe('unversioned move', () => {
    function move(source: string, destination: string) {
      return app.inject({
        method: 'POST',
        url: '/object/move',
        headers: { authorization },
        payload: { bucketId, sourceKey: source, destinationKey: destination },
      })
    }

    it('rejects a move denied by an UPDATE trigger and keeps the source in place', async () => {
      const source = `move-src-${randomUUID()}.txt`
      const destination = `move-dst-${randomUUID()}.txt`
      const upload1 = await upload(source, 'source')
      expect(upload1.statusCode).toBe(200)
      const sourceVersion = versions[0]
      await installTrigger('UPDATE', `NEW.name = '${destination}'`)

      const response = await move(source, destination)

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(destination)).toEqual([])
      expect((await rowsFor(source)).map((row) => row.version)).toEqual([sourceVersion])
      await expectBytes(source, sourceVersion)
    })

    it('still reports a missing source as not found', async () => {
      await installTrigger('UPDATE', 'true')

      const response = await move(`missing-${randomUUID()}.txt`, `move-dst-${randomUUID()}.txt`)

      expect(response.statusCode).toBe(400)
      expect(response.json()).toMatchObject({ statusCode: '404', code: 'NoSuchKey' })
    })
  })

  describe('delete markers', () => {
    async function seedVersionedObject() {
      await store.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
      const name = `marker-${randomUUID()}.txt`
      const response = await upload(name, 'original')
      expect(response.statusCode).toBe(200)
      await installTrigger('INSERT', 'NEW.is_delete_marker')
      return { name, version: versions[0] }
    }

    it('rejects a delete when the delete marker is blocked and keeps the object current', async () => {
      const { name, version } = await seedVersionedObject()

      const response = await app.inject({
        method: 'DELETE',
        url: `/object/${bucketId}/${name}`,
        headers: { authorization },
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(name)).toEqual([{ version, archived_at: null }])
      await expectBytes(name, version)
    })

    it('rejects a bulk delete when a delete marker is blocked and keeps the object current', async () => {
      const { name, version } = await seedVersionedObject()

      const response = await app.inject({
        method: 'DELETE',
        url: `/object/${bucketId}`,
        headers: { authorization },
        payload: { prefixes: [name] },
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(name)).toEqual([{ version, archived_at: null }])
      await expectBytes(name, version)
    })
  })
})
