import { randomUUID } from 'node:crypto'
import { FastifyInstance } from 'fastify'
import FormData from 'form-data'
import fs from 'fs'
import app from '../app'
import { useMockObject, useMockQueue } from './common'
import { useStorage, withDeleteEnabled } from './utils/storage'

/**
 * Delete markers are rows a principal creates, so they must carry that
 * principal as their owner. Under an owner-scoped policy an ownerless marker
 * fails the INSERT check when deleting a missing key, and later fails the
 * UPDATE check of the upsert probe when the same user uploads over it.
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
