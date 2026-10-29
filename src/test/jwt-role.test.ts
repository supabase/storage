import { randomUUID } from 'node:crypto'
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { signJWT } from '@internal/auth'
import { PgTenantConnection } from '@internal/database'
import type { FastifyInstance } from 'fastify'
import { Pool } from 'pg'
import app from '../app'
import { getConfig } from '../config'
import { useMockQueue } from './common'
import { useStorage, withDeleteEnabled } from './utils/storage'

describe('JWT database roles', () => {
  useMockQueue()
  const helper = useStorage()
  const config = getConfig()
  const customRole = `storage_role_${randomUUID().replaceAll('-', '')}`
  const userId = randomUUID()
  const body = 'private role regression fixture'
  let server: FastifyInstance
  let admin: Pool
  let bucketId: string
  let createdBucketId: string
  let serviceToken: string

  beforeAll(async () => {
    admin = new Pool({ connectionString: config.databaseURL })
    // Inherit ordinary table grants while retaining a distinct role and RLS.
    await admin.query(`CREATE ROLE "${customRole}" NOLOGIN INHERIT NOBYPASSRLS`)
    await admin.query(`GRANT authenticated TO "${customRole}"`)
    const { rows } = await admin.query('SELECT quote_ident(current_user) AS login')
    await admin.query(`GRANT "${customRole}" TO ${rows[0].login}`)
    serviceToken = await config.serviceKeyAsync
    server = app()
    await server.ready()
  })

  beforeEach(async () => {
    bucketId = `role-${randomUUID()}`
    createdBucketId = `role-${randomUUID()}`
    await helper.storage.createBucket({ id: bucketId, name: bucketId, public: false })
    await admin.query(`
      CREATE POLICY "${customRole}" ON storage.objects TO "${customRole}"
      USING (bucket_id = '${bucketId}') WITH CHECK (bucket_id = '${bucketId}');
      CREATE POLICY "${customRole}" ON storage.buckets TO "${customRole}"
      USING (id IN ('${bucketId}', '${createdBucketId}'))
      WITH CHECK (id IN ('${bucketId}', '${createdBucketId}'));
    `)
    const seeded = await upload(serviceToken, 'seed.txt')
    expect(seeded.statusCode, seeded.body).toBe(200)
  })

  afterEach(async () => {
    await admin.query(`
      DROP POLICY IF EXISTS "${customRole}" ON storage.objects;
      DROP POLICY IF EXISTS "${customRole}" ON storage.buckets;
    `)
    const objects = await admin.query(
      'SELECT name, version FROM storage.objects WHERE bucket_id = $1',
      [bucketId]
    )
    await helper.adapter.deleteObjects(
      config.storageS3Bucket,
      objects.rows.map((object) =>
        helper.storage.location.getKeyLocation({
          tenantId: config.tenantId,
          bucketId,
          objectName: object.name,
          version: object.version,
        })
      )
    )
    await withDeleteEnabled(helper.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.buckets WHERE id = ANY($1::text[])', [
        [bucketId, createdBucketId],
      ])
    })
  })

  afterAll(async () => {
    await server?.close()
    await admin.query(`DROP ROLE "${customRole}"`)
    await admin.end()
  })

  function upload(token: string, name: string) {
    return server.inject({
      method: 'POST',
      url: `/object/${bucketId}/${name}`,
      headers: { authorization: `Bearer ${token}`, 'content-type': 'text/plain' },
      payload: body,
    })
  }

  const roles = [
    { label: 'none', role: 'none', status: 403 },
    { label: 'authenticated', role: 'authenticated', status: 400 },
    { label: 'anon', role: 'anon', status: 400 },
    { label: 'empty', role: '', status: 400 },
    { label: 'missing', role: undefined, status: 400 },
    { label: 'service_role', role: config.dbServiceRole, status: 200 },
    { label: 'custom role', role: customRole, status: 200 },
  ]

  it.each(
    roles.flatMap((role) =>
      (['read', 'upload', 'create bucket'] as const).map((operation) => ({ ...role, operation }))
    )
  )('$label: $operation returns $status', async ({ role, operation, status }) => {
    const token = await signJWT({ sub: userId, role }, config.jwtSecret, '1h')
    const response =
      operation === 'read'
        ? await server.inject({
            method: 'GET',
            url: `/object/authenticated/${bucketId}/seed.txt`,
            headers: { authorization: `Bearer ${token}` },
          })
        : operation === 'upload'
          ? await upload(token, 'upload.txt')
          : await server.inject({
              method: 'POST',
              url: '/bucket',
              headers: { authorization: `Bearer ${token}` },
              payload: { name: createdBucketId },
            })

    expect(response.statusCode, response.body).toBe(status)
    if (role === 'none') {
      expect(response.json()).toMatchObject({ code: 'AccessDenied' })
    }
    if (operation === 'read' && status === 200) {
      expect(response.body).toBe(body)
    } else if (operation === 'upload') {
      const result = await admin.query(
        'SELECT owner_id FROM storage.objects WHERE bucket_id = $1 AND name = $2',
        [bucketId, 'upload.txt']
      )
      expect(result.rows).toEqual(status === 200 ? [{ owner_id: userId }] : [])
    } else if (operation === 'create bucket') {
      const result = await admin.query('SELECT owner_id FROM storage.buckets WHERE id = $1', [
        createdBucketId,
      ])
      expect(result.rows).toEqual(status === 200 ? [{ owner_id: userId }] : [])
    }
  })

  it.each([
    { role: config.dbServiceRole, rls: false },
    { role: customRole, rls: true },
  ])('applies $role with RLS active=$rls', async ({ role, rls }) => {
    const user = { jwt: '', payload: { role, sub: userId } }
    const connection = PgTenantConnection.create({
      tenantId: config.tenantId,
      dbUrl: config.databasePoolURL || config.databaseURL,
      maxConnections: 1,
      user,
      superUser: { jwt: serviceToken, payload: { role: config.dbServiceRole } },
    })
    const transaction = await connection.transaction()
    try {
      await connection.setScope(transaction)
      const result = await transaction.query(
        "SELECT current_user AS role, row_security_active('storage.objects') AS rls"
      )
      expect(result.rows).toEqual([{ role, rls }])
    } finally {
      await transaction.rollback()
      connection.dispose()
    }
  })

  it('rejects role none through TUS authorization', async () => {
    const token = await signJWT({ sub: userId, role: 'none' }, config.jwtSecret, '1h')
    const response = await server.inject({
      method: 'POST',
      url: '/upload/resumable',
      headers: {
        authorization: `Bearer ${token}`,
        'tus-resumable': '1.0.0',
        'upload-length': '1',
        'upload-metadata': `bucketName ${Buffer.from(bucketId).toString('base64')},objectName ${Buffer.from('tus.txt').toString('base64')}`,
      },
    })
    expect(response.statusCode, response.body).toBe(403)
    expect(response.body).toContain('Invalid role')
  })

  it('rejects role none through S3 session-token authorization', async () => {
    await server.listen({ port: 0, host: '127.0.0.1' })
    const address = server.server.address()
    if (!address || typeof address === 'string') throw new Error('Missing server port')
    const client = new S3Client({
      endpoint: `http://127.0.0.1:${address.port}/s3`,
      region: config.storageS3Region,
      forcePathStyle: true,
      maxAttempts: 1,
      credentials: {
        accessKeyId: config.tenantId,
        secretAccessKey: await config.anonKeyAsync,
        sessionToken: await signJWT({ sub: userId, role: 'none' }, config.jwtSecret, '1h'),
      },
    })
    try {
      await expect(
        client.send(new GetObjectCommand({ Bucket: bucketId, Key: 'seed.txt' }))
      ).rejects.toMatchObject({ name: 'AccessDenied', $metadata: { httpStatusCode: 403 } })
    } finally {
      client.destroy()
    }
  })
})
