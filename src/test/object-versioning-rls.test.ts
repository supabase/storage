import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { PgTenantConnection } from '@internal/database'
import { ErrorCode } from '@internal/errors'
import { FastifyInstance } from 'fastify'
import FormData from 'form-data'
import fs from 'fs'
import app from '../app'
import { getConfig } from '../config'
import type { StorageBackendAdapter } from '../storage/backend'
import { StoragePgDB } from '../storage/database'
import {
  ObjectAdminDelete,
  ObjectCreatedCopyEvent,
  ObjectCreatedMove,
  ObjectCreatedPostEvent,
  ObjectCreatedPutEvent,
  ObjectRemovedMove,
} from '../storage/events'
import { ObjectStorage } from '../storage/object'
import { Uploader } from '../storage/uploader'
import { useMockObject, useMockQueue } from './common'
import { useStorage, withDeleteEnabled } from './utils/storage'

/**
 * Delete markers are rows a principal creates, so they must carry that
 * principal as their owner. Deleting a missing key is governed by the DELETE
 * policy, evaluated against the marker the delete would write: under an
 * owner-scoped policy an ownerless marker fails that check, and later fails
 * the UPDATE check of the upsert probe when the same user uploads over it.
 */
describe('object versioning - delete markers under owner-scoped RLS', () => {
  useMockObject()
  useMockQueue()
  const tHelper = useStorage()

  let appInstance: FastifyInstance
  let userToken: string
  let userId: string
  let bucketId: string
  let policyName: string

  beforeAll(() => {
    appInstance = app()
    userToken = process.env.AUTHENTICATED_KEY as string
    userId = JSON.parse(Buffer.from(userToken.split('.')[1], 'base64').toString()).sub
  })

  afterAll(async () => {
    await appInstance.close()
  })

  beforeEach(async () => {
    bucketId = `versioning-rls-${randomUUID()}`
    policyName = `versioning_rls_${randomUUID().replaceAll('-', '_')}`
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.connection.query(`
      CREATE POLICY "${policyName}" ON storage.objects FOR ALL
      USING (bucket_id = '${bucketId}' AND owner = auth.uid())
    `)
  })

  afterEach(async () => {
    await tHelper.database.connection.query(
      `DROP POLICY IF EXISTS "${policyName}" ON storage.objects`
    )
    await withDeleteEnabled(tHelper.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
    })
  })

  const authorization = () => ({ authorization: `Bearer ${userToken}` })

  const upload = (name: string) => {
    const form = new FormData()
    form.append('file', fs.createReadStream('./src/test/assets/sadcat.jpg'))
    return appInstance.inject({
      method: 'POST',
      url: `/object/${bucketId}/${name}`,
      headers: { ...form.getHeaders(), ...authorization() },
      payload: form,
    })
  }

  const currentRow = (name: string) =>
    tHelper.database.findObject(bucketId, name, 'owner,owner_id,is_delete_marker')

  it('lets the owner delete a key that does not exist yet', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: `/object/${bucketId}/missing.txt`,
      headers: authorization(),
    })

    expect(response.statusCode).toBe(200)
    await expect(currentRow('missing.txt')).resolves.toMatchObject({
      is_delete_marker: true,
      owner: userId,
      owner_id: userId,
    })
  })

  it('lets the owner bulk delete keys that do not exist yet', async () => {
    const names = ['missing-a.txt', 'missing-b.txt']
    const response = await appInstance.inject({
      method: 'DELETE',
      url: `/object/${bucketId}`,
      headers: authorization(),
      payload: { prefixes: names },
    })

    expect(response.statusCode).toBe(200)
    const deletedNames = (await response.json()).map((object: { name: string }) => object.name)
    expect(deletedNames.sort()).toEqual(names)
    for (const name of names) {
      await expect(currentRow(name)).resolves.toMatchObject({
        is_delete_marker: true,
        owner: userId,
        owner_id: userId,
      })
    }
  })

  it('lets the owner upload again over their own delete marker', async () => {
    expect((await upload('file.txt')).statusCode).toBe(200)

    const deleted = await appInstance.inject({
      method: 'DELETE',
      url: `/object/${bucketId}/file.txt`,
      headers: authorization(),
    })
    expect(deleted.statusCode).toBe(200)
    await expect(currentRow('file.txt')).resolves.toMatchObject({
      is_delete_marker: true,
      owner: userId,
    })

    // A non-upsert upload over a delete marker is authorized with the upsert
    // probe, whose ON CONFLICT DO UPDATE checks the policy against the marker.
    const again = await upload('file.txt')
    expect(again.statusCode).toBe(200)
    await expect(currentRow('file.txt')).resolves.toMatchObject({
      is_delete_marker: false,
      owner: userId,
    })
  })
})

describe.each([
  'DISABLED',
  'ENABLED',
  'SUSPENDED',
] as const)('final object authorization (%s)', (versioningStatus) => {
  const helper = useStorage()
  const alice = randomUUID()
  const bob = randomUUID()
  let connection: PgTenantConnection
  let callerDb: StoragePgDB
  let bucketId: string
  let policyName: string
  let uploadBytes: StorageBackendAdapter['uploadObject']

  beforeAll(() => {
    const config = getConfig()
    connection = PgTenantConnection.create({
      tenantId: config.tenantId,
      dbUrl: config.databaseURL,
      maxConnections: 5,
      user: { jwt: '', payload: { role: config.dbAuthenticatedRole, sub: alice } },
      superUser: { jwt: '', payload: { role: config.dbServiceRole } },
    })
    callerDb = new StoragePgDB(connection, { tenantId: config.tenantId, host: 'localhost' })
  })

  afterAll(() => connection.dispose())

  beforeEach(async () => {
    uploadBytes = helper.adapter.uploadObject.bind(helper.adapter)
    bucketId = `write-auth-${randomUUID()}`
    policyName = `write_auth_${randomUUID().replaceAll('-', '_')}`
    await helper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: versioningStatus === 'SUSPENDED' ? 'ENABLED' : versioningStatus,
    })
    if (versioningStatus === 'SUSPENDED') {
      await helper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    }
    await helper.database.connection.query(`
        CREATE POLICY "${policyName}" ON storage.objects FOR ALL
          USING (bucket_id = '${bucketId}');
        CREATE POLICY "${policyName}_owner" ON storage.objects AS RESTRICTIVE FOR ALL
          TO authenticated
          USING (bucket_id <> '${bucketId}' OR owner_id = current_setting('request.jwt.claim.sub', true));
      `)
    vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    for (const event of [
      ObjectCreatedCopyEvent,
      ObjectCreatedMove,
      ObjectCreatedPostEvent,
      ObjectCreatedPutEvent,
      ObjectRemovedMove,
    ]) {
      vi.spyOn(event, 'sendWebhook').mockResolvedValue(undefined)
    }
  })

  afterEach(async () => {
    await helper.database.connection.query(`
        DROP POLICY IF EXISTS "${policyName}" ON storage.objects;
        DROP POLICY IF EXISTS "${policyName}_owner" ON storage.objects;
        DROP POLICY IF EXISTS "${policyName}_metadata" ON storage.objects;
      `)
    await withDeleteEnabled(helper.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
    })
  })

  async function seed(name: string, owner = alice, userMetadata = {}) {
    const version = randomUUID()
    const metadata = await uploadBytes(
      helper.storage.location.getRootLocation(),
      helper.storage.location.getKeyLocation({
        tenantId: callerDb.tenantId,
        bucketId,
        objectName: name,
      }),
      version,
      Readable.from(['fixture!']),
      'text/plain',
      'no-cache'
    )
    return helper.database.upsertObject({
      bucket_id: bucketId,
      name,
      owner,
      version,
      metadata,
      user_metadata: userMetadata,
    })
  }

  const current = (name: string) =>
    helper.database.findObject(bucketId, name, 'version,owner_id,user_metadata', {
      dontErrorOnEmpty: true,
    })

  const objects = () =>
    new ObjectStorage(helper.adapter, callerDb, helper.storage.location, bucketId)

  function upload(isUpsert = true) {
    return new Uploader(helper.adapter, callerDb, helper.storage.location).upload({
      bucketId,
      objectName: 'target.txt',
      owner: alice,
      isUpsert,
      uploadType: 'standard',
      file: {
        body: Readable.from(['fixture!']),
        mimeType: 'text/plain',
        cacheControl: 'no-cache',
        isTruncated: () => false,
      },
    })
  }

  it('allows an ordinary owner upload', async () => {
    await upload()
    await expect(current('target.txt')).resolves.toMatchObject({ owner_id: alice })
  })

  it.each([
    'upload',
    'copy',
  ] as const)('rejects an upsert %s when another owner creates the destination during transfer', async (kind) => {
    let replacement: Awaited<ReturnType<typeof seed>> | undefined
    if (kind === 'upload') {
      vi.spyOn(helper.adapter, 'uploadObject').mockImplementation(async (...args) => {
        replacement = await seed('target.txt', bob)
        return uploadBytes(...args)
      })
    } else {
      await seed('source.txt')
      const copy = helper.adapter.copyObject.bind(helper.adapter)
      vi.spyOn(helper.adapter, 'copyObject').mockImplementation(async (...args) => {
        replacement = await seed('target.txt', bob)
        return copy(...args)
      })
    }

    const operation =
      kind === 'upload'
        ? upload()
        : objects().copyObject({
            sourceKey: 'source.txt',
            destinationBucket: bucketId,
            destinationKey: 'target.txt',
            owner: alice,
            upsert: true,
            uploadType: 'standard',
          })
    await expect(operation).rejects.toMatchObject({ code: ErrorCode.AccessDenied })
    expect(replacement).toBeDefined()
    await expect(current('target.txt')).resolves.toMatchObject({
      owner_id: bob,
      version: replacement?.version,
    })
  })

  it('rejects moving a source replaced with another owner during a backend retry', async () => {
    await seed('source.txt')
    let copies = 0
    let replacement: Awaited<ReturnType<typeof seed>> | undefined
    const copy = helper.adapter.copyObject.bind(helper.adapter)
    vi.spyOn(helper.adapter, 'copyObject').mockImplementation(async (...args) => {
      if (copies++ === 0) {
        replacement = await seed('source.txt', bob)
        await helper.adapter.deleteObject(args[0], args[1], args[2])
      }
      return copy(...args)
    })

    await expect(
      objects().moveObject('source.txt', bucketId, 'target.txt', 'standard', alice)
    ).rejects.toMatchObject({
      code: versioningStatus === 'DISABLED' ? ErrorCode.NoSuchKey : ErrorCode.AccessDenied,
    })
    expect(copies).toBe(2)
    expect(replacement).toBeDefined()
    await expect(current('source.txt')).resolves.toMatchObject({
      owner_id: bob,
      version: replacement?.version,
    })
    await expect(current('target.txt')).resolves.toBeUndefined()
  })

  it('checks refreshed copy metadata against the destination policy', async () => {
    await seed('source.txt', alice, { allowed: true })
    await helper.database.connection.query(`
        CREATE POLICY "${policyName}_metadata" ON storage.objects AS RESTRICTIVE FOR INSERT
          TO authenticated WITH CHECK (name <> 'target.txt' OR user_metadata->>'allowed' = 'true')
      `)
    let copies = 0
    const copy = helper.adapter.copyObject.bind(helper.adapter)
    vi.spyOn(helper.adapter, 'copyObject').mockImplementation(async (...args) => {
      if (copies++ === 0) {
        await seed('source.txt', alice, { allowed: false })
        await helper.adapter.deleteObject(args[0], args[1], args[2])
      }
      return copy(...args)
    })

    await expect(
      objects().copyObject({
        sourceKey: 'source.txt',
        destinationBucket: bucketId,
        destinationKey: 'target.txt',
        owner: alice,
        upsert: false,
        uploadType: 'standard',
      })
    ).rejects.toMatchObject({ code: ErrorCode.AccessDenied })
    expect(copies).toBe(2)
    await expect(current('source.txt')).resolves.toMatchObject({
      user_metadata: { allowed: false },
    })
    await expect(current('target.txt')).resolves.toBeUndefined()
  })

  it('checks the actual uploaded size against the metadata policy', async () => {
    await helper.database.connection.query(`
        CREATE POLICY "${policyName}_metadata" ON storage.objects AS RESTRICTIVE FOR INSERT
          TO authenticated WITH CHECK (coalesce((metadata->>'size')::bigint, 0) <= 5)
      `)

    await expect(upload(false)).rejects.toMatchObject({ code: ErrorCode.AccessDenied })
    await expect(current('target.txt')).resolves.toBeUndefined()
  })
})
