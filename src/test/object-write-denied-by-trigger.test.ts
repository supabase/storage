import { randomUUID } from 'node:crypto'
import type { FastifyInstance } from 'fastify'
import buildApp from '../app'
import { getConfig } from '../config'
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
  async function installTrigger(event: string, condition: string, action = 'RETURN NULL') {
    const suffix = randomUUID().replaceAll('-', '')
    const name = `deny_trg_${suffix}`
    const fn = `deny_fn_${suffix}`
    await store.database.connection.query(`
      CREATE FUNCTION ${fn}() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.bucket_id = '${bucketId}' AND (${condition}) THEN
          ${action};
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

  describe('a trigger that rewrites the key', () => {
    it('rejects an upload stored under another name and removes the bytes', async () => {
      await installTrigger('INSERT', 'true', `NEW.name := NEW.name || '-renamed'; RETURN NEW`)
      const name = `rewritten-${randomUUID()}.txt`

      const response = await upload(name, 'hello')

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(name)).toEqual([])
      expect(await rowsFor(`${name}-renamed`)).toEqual([])
      for (const version of versions) {
        await expectNoBytes(name, version)
      }
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

    it('rejects an overwrite whose BEFORE UPDATE trigger returns the old row and keeps the original', async () => {
      const name = `upsert-old-row-${randomUUID()}.txt`
      expect((await upload(name, 'original')).statusCode).toBe(200)
      const [originalVersion] = versions
      await installTrigger('UPDATE', 'true', 'RETURN OLD')

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

    it.each([
      ['returns the old row', 'RETURN OLD', ''],
      ['renames the destination', `NEW.name := NEW.name || '-renamed'; RETURN NEW`, '-renamed'],
    ])(
      'rejects a move when the trigger %s and leaves no row under the wrong key',
      async (_, action, suffix) => {
        const source = `move-src-${randomUUID()}.txt`
        const destination = `move-dst-${randomUUID()}.txt`
        expect((await upload(source, 'source')).statusCode).toBe(200)
        const sourceVersion = versions[0]
        await installTrigger('UPDATE', `NEW.name = '${destination}'`, action)

        const response = await move(source, destination)

        expect(response.statusCode).toBe(400)
        expect(response.json()).toEqual(rejected)
        expect(await rowsFor(`${destination}${suffix}`)).toEqual([])
        expect(await rowsFor(destination)).toEqual([])
        expect((await rowsFor(source)).map((row) => row.version)).toEqual([sourceVersion])
        await expectBytes(source, sourceVersion)
      }
    )

    it('still reports a missing source as not found', async () => {
      await installTrigger('UPDATE', 'true')

      const response = await move(`missing-${randomUUID()}.txt`, `move-dst-${randomUUID()}.txt`)

      expect(response.statusCode).toBe(400)
      expect(response.json()).toMatchObject({ statusCode: '404', code: 'NoSuchKey' })
    })
  })

  describe('unversioned move by a caller whose update policy hides the source', () => {
    it('reports the source as not found even when an UPDATE trigger raises', async () => {
      const source = `move-rls-src-${randomUUID()}.txt`
      const destination = `move-rls-dst-${randomUUID()}.txt`
      expect((await upload(source, 'source')).statusCode).toBe(200)
      const policy = `rls_select_${randomUUID().replaceAll('-', '')}`
      await store.database.connection.query(
        `CREATE POLICY ${policy} ON storage.objects FOR SELECT TO authenticated USING (bucket_id = '${bucketId}')`
      )
      try {
        await installTrigger('UPDATE', 'true', `RAISE EXCEPTION 'blocked by tenant trigger'`)

        const response = await app.inject({
          method: 'POST',
          url: '/object/move',
          headers: { authorization: `Bearer ${process.env.AUTHENTICATED_KEY}` },
          payload: { bucketId, sourceKey: source, destinationKey: destination },
        })

        expect(response.statusCode).toBe(400)
        expect(response.json()).toMatchObject({ statusCode: '404', code: 'NoSuchKey' })
        expect(await rowsFor(destination)).toEqual([])
      } finally {
        await store.database.connection.query(`DROP POLICY IF EXISTS ${policy} ON storage.objects`)
      }
    })
  })

  describe('promotion after deleting the current version', () => {
    async function seedTwoVersions(action?: string) {
      await store.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
      const name = `promote-${randomUUID()}.txt`
      expect((await upload(name, 'first')).statusCode).toBe(200)
      expect((await upload(name, 'second', { upsert: true })).statusCode).toBe(200)
      const [older, current] = versions
      await installTrigger(
        'UPDATE',
        'OLD.archived_at IS NOT NULL AND NEW.archived_at IS NULL',
        action
      )
      return { name, older, current }
    }

    async function expectUntouched(name: string, older: string, current: string) {
      const rows = (await rowsFor(name)).sort((a, b) => a.version.localeCompare(b.version))
      expect(rows.find((row) => row.version === current)?.archived_at).toBeNull()
      expect(rows.find((row) => row.version === older)?.archived_at).not.toBeNull()
      expect(rows).toHaveLength(2)
      await expectBytes(name, current)
    }

    it('rejects deleting the current version, by id and in bulk, and keeps both versions as they were', async () => {
      const { name, older, current } = await seedTwoVersions()

      const single = await app.inject({
        method: 'DELETE',
        url: `/object/${bucketId}/${name}?versionId=${current}`,
        headers: { authorization },
      })
      const bulk = await app.inject({
        method: 'DELETE',
        url: `/object/${bucketId}`,
        headers: { authorization },
        payload: { prefixes: [{ path: name, versionId: current }] },
      })

      for (const response of [single, bulk]) {
        expect(response.statusCode).toBe(400)
        expect(response.json()).toEqual(rejected)
      }
      await expectUntouched(name, older, current)
    })

    it.each([
      ['returns the old row', 'RETURN OLD'],
      ['renames the row', `NEW.name := NEW.name || '-renamed'; RETURN NEW`],
    ])('rejects deleting the current version when the trigger %s', async (_, action) => {
      const { name, older, current } = await seedTwoVersions(action)

      const single = await app.inject({
        method: 'DELETE',
        url: `/object/${bucketId}/${name}?versionId=${current}`,
        headers: { authorization },
      })
      const bulk = await app.inject({
        method: 'DELETE',
        url: `/object/${bucketId}`,
        headers: { authorization },
        payload: { prefixes: [{ path: name, versionId: current }] },
      })

      for (const response of [single, bulk]) {
        expect(response.statusCode).toBe(400)
        expect(response.json()).toEqual(rejected)
      }
      expect(await rowsFor(`${name}-renamed`)).toEqual([])
      await expectUntouched(name, older, current)
    })

    it('still deletes the only version of a key', async () => {
      await store.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
      const name = `promote-only-${randomUUID()}.txt`
      expect((await upload(name, 'only')).statusCode).toBe(200)
      await installTrigger('UPDATE', 'OLD.archived_at IS NOT NULL AND NEW.archived_at IS NULL')

      const response = await app.inject({
        method: 'DELETE',
        url: `/object/${bucketId}/${name}?versionId=${versions[0]}`,
        headers: { authorization },
      })

      expect(response.statusCode).toBe(200)
      expect(await rowsFor(name)).toEqual([])
    })
  })

  describe('unversioned move denied only at the final write', () => {
    it('rejects the move and keeps the source when the write after the probe is skipped', async () => {
      const source = `move-late-src-${randomUUID()}.txt`
      const destination = `move-late-dst-${randomUUID()}.txt`
      expect((await upload(source, 'source')).statusCode).toBe(200)
      const sourceVersion = versions[0]
      const sequence = `late_move_${randomUUID().replaceAll('-', '')}`
      await store.database.connection.query(`CREATE SEQUENCE ${sequence}`)
      await store.database.connection.query(`GRANT USAGE ON SEQUENCE ${sequence} TO PUBLIC`)
      try {
        await installTrigger('UPDATE', `NEW.name = '${destination}' AND nextval('${sequence}') > 2`)

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
        await expectBytes(source, sourceVersion)
      } finally {
        await store.database.connection.query(`DROP SEQUENCE IF EXISTS ${sequence} CASCADE`)
      }
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

    it.each([
      [
        'a single delete',
        (name: string) => ({ url: `/object/${bucketId}/${name}`, payload: undefined }),
      ],
      [
        'a bulk delete',
        (name: string) => ({ url: `/object/${bucketId}`, payload: { prefixes: [name] } }),
      ],
    ])('rejects %s when the marker is stored under another name', async (_, request) => {
      await store.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
      const name = `marker-renamed-${randomUUID()}.txt`
      expect((await upload(name, 'original')).statusCode).toBe(200)
      const [version] = versions
      await installTrigger(
        'INSERT',
        'NEW.is_delete_marker',
        `NEW.name := NEW.name || '-renamed'; RETURN NEW`
      )

      const response = await app.inject({
        method: 'DELETE',
        headers: { authorization },
        ...request(name),
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual(rejected)
      expect(await rowsFor(name)).toEqual([{ version, archived_at: null }])
      expect(await rowsFor(`${name}-renamed`)).toEqual([])
      await expectBytes(name, version)
    })

    describe('when the archive update is blocked', () => {
      async function seedAndBlockArchive(status: 'ENABLED' | 'SUSPENDED') {
        await store.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
        const name = `archive-${randomUUID()}.txt`
        const response = await upload(name, 'original')
        expect(response.statusCode).toBe(200)
        await store.database.updateBucket(bucketId, { versioning_status: status })
        await installTrigger('UPDATE', 'NEW.archived_at IS NOT NULL')
        return { name, version: versions[0] }
      }

      it.each(['ENABLED', 'SUSPENDED'] as const)(
        'rejects an overwrite on a %s bucket and keeps the object current',
        async (status) => {
          const { name, version } = await seedAndBlockArchive(status)
          const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send')

          const response = await upload(name, 'replacement', { upsert: true })

          expect(response.statusCode).toBe(400)
          expect(response.json()).toEqual(rejected)
          expect(await rowsFor(name)).toEqual([{ version, archived_at: null }])
          expect(deleteSpy).toHaveBeenCalledExactlyOnceWith(
            expect.objectContaining({ bucketId, name })
          )
          await expectBytes(name, version)
        }
      )

      it.each(['ENABLED', 'SUSPENDED'] as const)(
        'still writes a new key on a %s bucket because there is nothing to archive',
        async (status) => {
          await seedAndBlockArchive(status)
          const name = `archive-new-${randomUUID()}.txt`

          const response = await upload(name, 'fresh')

          expect(response.statusCode).toBe(200)
          expect(await rowsFor(name)).toHaveLength(1)
        }
      )

      it('rejects an overwriting copy and removes the copied bytes', async () => {
        await store.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
        const source = `archive-copy-src-${randomUUID()}.txt`
        const destination = `archive-copy-dst-${randomUUID()}.txt`
        expect((await upload(source, 'source')).statusCode).toBe(200)
        const sourceVersion = versions[0]
        expect((await upload(destination, 'existing')).statusCode).toBe(200)
        const destinationVersion = versions[1]
        await installTrigger(
          'UPDATE',
          `NEW.archived_at IS NOT NULL AND NEW.name = '${destination}'`
        )
        const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send')

        const response = await app.inject({
          method: 'POST',
          url: '/object/copy',
          headers: { authorization, 'x-upsert': 'true' },
          payload: { bucketId, sourceKey: source, destinationKey: destination },
        })

        expect(response.statusCode).toBe(400)
        expect(response.json()).toEqual(rejected)
        expect(await rowsFor(destination)).toEqual([
          { version: destinationVersion, archived_at: null },
        ])
        expect(deleteSpy).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ bucketId, name: destination })
        )
        const { version } = deleteSpy.mock.calls[0][0] as { version?: string }
        expect(version).not.toBe(destinationVersion)
        await expectNoBytes(destination, version as string)
        await expectBytes(source, sourceVersion)
        await expectBytes(destination, destinationVersion)
      })

      it('rejects the whole bulk delete when only one of the archives is blocked', async () => {
        await store.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
        const blocked = `archive-blocked-${randomUUID()}.txt`
        const allowed = `archive-allowed-${randomUUID()}.txt`
        expect((await upload(blocked, 'one')).statusCode).toBe(200)
        expect((await upload(allowed, 'two')).statusCode).toBe(200)
        const [blockedVersion, allowedVersion] = versions
        await installTrigger('UPDATE', `NEW.archived_at IS NOT NULL AND NEW.name = '${blocked}'`)

        const response = await app.inject({
          method: 'DELETE',
          url: `/object/${bucketId}`,
          headers: { authorization },
          payload: { prefixes: [allowed, blocked] },
        })

        expect(response.statusCode).toBe(400)
        expect(response.json()).toEqual(rejected)
        expect(await rowsFor(blocked)).toEqual([{ version: blockedVersion, archived_at: null }])
        expect(await rowsFor(allowed)).toEqual([{ version: allowedVersion, archived_at: null }])
      })

      it.each(['ENABLED', 'SUSPENDED'] as const)(
        'rejects a single delete on a %s bucket and keeps the object current',
        async (status) => {
          const { name, version } = await seedAndBlockArchive(status)

          const response = await app.inject({
            method: 'DELETE',
            url: `/object/${bucketId}/${name}`,
            headers: { authorization },
          })

          expect(response.statusCode).toBe(400)
          expect(response.json()).toEqual(rejected)
          expect(await rowsFor(name)).toEqual([{ version, archived_at: null }])
          await expectBytes(name, version)
        }
      )

      it.each(['ENABLED', 'SUSPENDED'] as const)(
        'rejects a bulk delete on a %s bucket and keeps the object current',
        async (status) => {
          const { name, version } = await seedAndBlockArchive(status)

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
        }
      )
    })
  })

  describe('owner update', () => {
    it.each([
      ['renames the row', `NEW.name := NEW.name || '-renamed'; RETURN NEW`],
      ['changes the version', `NEW.version := 'changed'; RETURN NEW`],
    ])('rejects it when the trigger %s and keeps the row as it was', async (_, action) => {
      const name = `owner-${randomUUID()}.txt`
      expect((await upload(name, 'original')).statusCode).toBe(200)
      const [version] = versions
      await installTrigger('UPDATE', `NEW.name = '${name}'`, action)

      await expect(
        store.storage.from(bucketId).updateObjectOwner(name, randomUUID())
      ).rejects.toMatchObject({ code: 'AccessDenied' })

      expect(await rowsFor(name)).toEqual([{ version, archived_at: null }])
      expect(await rowsFor(`${name}-renamed`)).toEqual([])
      await expectBytes(name, version)
    })
  })
})
