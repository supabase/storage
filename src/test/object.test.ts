vi.hoisted(() => {
  process.env.PG_QUEUE_ENABLE = 'true'
})

import {
  generateHS512JWK,
  getMaxNumericJWTExpiration,
  SIGNED_URL_SCOPE_DOWNLOAD,
  SIGNED_URL_SCOPE_UPLOAD,
  SignedToken,
  signJWT,
  verifyJWT,
} from '@internal/auth'
import {
  type DatabaseTransaction,
  getPostgresConnection,
  getServiceKeyUser,
} from '@internal/database'
import { ErrorCode, StorageBackendError } from '@internal/errors'
import { MAX_OBJECTS_PER_REQUEST } from '@storage/limits'
import { randomUUID } from 'crypto'
import { FastifyInstance } from 'fastify'
import FormData from 'form-data'
import fs from 'fs'
import app from '../app'
import { getConfig, JwksConfig, JwksConfigKeyOCT, mergeConfig } from '../config'
import { backends, Obj } from '../storage'
import { ObjectAdminDelete } from '../storage/events'
import { useMockObject, useMockQueue } from './common'
import { useStorage, withDeleteEnabled } from './utils/storage'

const { jwtSecret, serviceKeyAsync, tenantId } = getConfig()
const anonKey = process.env.ANON_KEY || ''
const S3Backend = backends.S3Backend
let appInstance: FastifyInstance

type SignedUrlResult = {
  error: string | null
  path: string
  signedURL: string | null
}

let tnx: DatabaseTransaction | undefined
async function getSuperuserPostgrestClient() {
  const superUser = await getServiceKeyUser(tenantId)

  const conn = await getPostgresConnection({
    superUser,
    user: superUser,
    tenantId,
    host: 'localhost',
  })
  tnx = await conn.transaction()

  return tnx
}

async function findObject(
  db: DatabaseTransaction,
  bucketId: string,
  name: string
): Promise<Obj | undefined> {
  const result = await db.query<Obj>({
    text: `
      SELECT *
      FROM objects
      WHERE bucket_id = $1
        AND name = $2
      LIMIT 1
    `,
    values: [bucketId, name],
  })

  return result.rows[0]
}

async function insertObjects(
  db: DatabaseTransaction,
  objects:
    | Array<Partial<Obj> & { bucket_id: string; name: string }>
    | (Partial<Obj> & { bucket_id: string; name: string })
) {
  const rows = Array.isArray(objects) ? objects : [objects]

  for (const row of rows) {
    const entries = Object.entries(row)
    await db.query({
      text: `
        INSERT INTO objects (${entries.map(([column]) => column).join(', ')})
        VALUES (${entries.map((_, index) => `$${index + 1}`).join(', ')})
      `,
      values: entries.map(([, value]) => value),
    })
  }
}

async function deleteObjectsByName(
  db: DatabaseTransaction,
  bucketId: string,
  names: string | string[]
) {
  await db.query({
    text: `
      DELETE FROM objects
      WHERE bucket_id = $1
        AND name = ANY($2::text[])
    `,
    values: [bucketId, Array.isArray(names) ? names : [names]],
  })
}

async function insertObjectNames(db: DatabaseTransaction, bucketId: string, names: string[]) {
  const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'
  const versions = names.map((_, index) => `test-version-${randomUUID()}-${index}`)

  await db.query({
    text: `
      INSERT INTO objects (bucket_id, name, owner, owner_id, version, metadata)
      SELECT $1, seeded.name, $2::uuid, $2::text, seeded.version, $3::jsonb
      FROM unnest($4::text[], $5::text[]) AS seeded(name, version)
    `,
    values: [bucketId, owner, { size: 1234 }, names, versions],
  })
}

async function insertBucket(
  db: DatabaseTransaction,
  bucket: {
    id: string
    name: string
    public: boolean
    file_size_limit: null
    allowed_mime_types: null
    type: string
  }
) {
  await db.query({
    text: `
      INSERT INTO buckets (id, name, public, file_size_limit, allowed_mime_types, type)
      VALUES ($1, $2, $3, $4, $5, $6)
    `,
    values: [
      bucket.id,
      bucket.name,
      bucket.public,
      bucket.file_size_limit,
      bucket.allowed_mime_types,
      bucket.type,
    ],
  })
}

useMockObject()
useMockQueue()

beforeEach(() => {
  getConfig({ reload: true })
  appInstance = app()
})

afterEach(async () => {
  if (tnx) {
    await tnx.commit()
  }
  await appInstance.close()
})

/*
 * GET /object/:id
 */
describe('testing GET object', () => {
  test('check if RLS policies are respected: authenticated user is able to read authenticated resource', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['x-robots-tag']).toBe('none')
    expect(response.headers['last-modified']).toBe('Thu, 12 Aug 2021 16:00:00 GMT')
    expect(S3Backend.prototype.getObject).toHaveBeenCalled()
  })

  test('check if RLS policies are respected: authenticated user is able to read authenticated resource without /authenticated prefix', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['last-modified']).toBe('Thu, 12 Aug 2021 16:00:00 GMT')
    expect(S3Backend.prototype.getObject).toHaveBeenCalled()
  })

  test('forward 304 and If-Modified-Since/If-None-Match headers', async () => {
    const mockGetObject = vi.spyOn(S3Backend.prototype, 'getObject')
    mockGetObject.mockRejectedValue({
      $metadata: {
        httpStatusCode: 304,
      },
    })
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        'if-modified-since': 'Thu, 12 Aug 2021 16:00:00 GMT',
        'if-none-match': 'abc',
      },
    })
    expect(response.statusCode).toBe(304)
    expect(mockGetObject.mock.calls[0][3]).toMatchObject({
      ifModifiedSince: 'Thu, 12 Aug 2021 16:00:00 GMT',
      ifNoneMatch: 'abc',
    })
  })

  test('get authenticated object info', async () => {
    const response = await appInstance.inject({
      method: 'HEAD',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['last-modified']).toBe('Wed, 12 Oct 2022 11:17:02 GMT')
    expect(response.headers['content-length']).toBe('3746')
    expect(response.headers['cache-control']).toBe('no-cache')
  })

  test('get authenticated object info without the /authenticated prefix', async () => {
    const response = await appInstance.inject({
      method: 'HEAD',
      url: '/object/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['last-modified']).toBe('Wed, 12 Oct 2022 11:17:02 GMT')
    expect(response.headers['content-length']).toBe('3746')
    expect(response.headers['cache-control']).toBe('no-cache')
  })

  test('get authenticated object info returns NoSuchKey for a missing object', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/info/authenticated/bucket2/authenticated/notfound-info.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })

    expect(response.statusCode).toBe(400)
    expect(response.json()).toEqual({
      statusCode: '404',
      error: 'not_found',
      message: 'Object not found',
    })
    expect(S3Backend.prototype.headObject).not.toHaveBeenCalled()
  })

  test('cannot get authenticated object info without the /authenticated prefix if no jwt is provided', async () => {
    const response = await appInstance.inject({
      method: 'HEAD',
      url: '/object/bucket2/authenticated/casestudy.png',
    })
    expect(response.statusCode).toBe(400)
  })

  test('get public object info without using the /public prefix', async () => {
    const response = await appInstance.inject({
      method: 'HEAD',
      url: '/object/public-bucket-2/favicon.ico',
      headers: {
        authorization: ``,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['last-modified']).toBe('Wed, 12 Oct 2022 11:17:02 GMT')
    expect(response.headers['content-length']).toBe('3746')
    expect(response.headers['cache-control']).toBe('no-cache')
  })

  test('get public object info', async () => {
    const response = await appInstance.inject({
      method: 'HEAD',
      url: '/object/public-bucket-2/favicon.ico',
      headers: {
        authorization: ``,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['last-modified']).toBe('Wed, 12 Oct 2022 11:17:02 GMT')
    expect(response.headers['content-length']).toBe('3746')
    expect(response.headers['cache-control']).toBe('no-cache')
  })

  test('force downloading file with default name', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png?download',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(S3Backend.prototype.getObject).toHaveBeenCalled()
    expect(response.headers).toEqual(
      expect.objectContaining({
        'content-disposition': `attachment;`,
      })
    )
  })

  test('force downloading file with a custom name', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png?download=testname.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(S3Backend.prototype.getObject).toHaveBeenCalled()
    expect(response.headers).toEqual(
      expect.objectContaining({
        'content-disposition': `attachment; filename=testname.png; filename*=UTF-8''testname.png`,
      })
    )
  })

  test('check if RLS policies are respected: anon user is not able to read authenticated resource', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.getObject).not.toHaveBeenCalled()
  })

  test('check if RLS policies are respected: anon user is not able to read authenticated resource without /authenticated prefix', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.getObject).not.toHaveBeenCalled()
  })

  test('user is not able to read a resource without Auth header', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png',
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.getObject).not.toHaveBeenCalled()
  })

  test('user is not able to read a resource without Auth header without the /authenticated prefix', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/bucket2/authenticated/casestudy.png',
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.getObject).not.toHaveBeenCalled()
  })

  test('return 400 when reading a non existent object', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/notfound',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.getObject).not.toHaveBeenCalled()
  })

  test('return 400 when reading a non existent bucket', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/authenticated/notfound/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.getObject).not.toHaveBeenCalled()
  })
})

/*
 * POST /object/:id
 * multipart upload
 */
describe('testing POST object via multipart upload', () => {
  test('check if RLS policies are respected: authenticated user is able to upload authenticated resource', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      'x-upsert': 'true',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/authenticated/casestudy1.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
    expect(await response.json()).toEqual(
      expect.objectContaining({
        Id: expect.any(String),
        Key: 'bucket2/authenticated/casestudy1.png',
      })
    )
  })

  test('check if RLS policies are respected: anon user is not able to upload authenticated resource', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
      'x-upsert': 'true',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/authenticated/casestudy.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
    expect(response.body).toBe(
      JSON.stringify({
        statusCode: '403',
        error: 'Unauthorized',
        message: 'new row violates row-level security policy',
      })
    )
  })

  test('check if RLS policies are respected: user is not able to upload a resource without Auth header', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/authenticated/casestudy.png',
      headers: form.getHeaders(),
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when uploading to a non existent bucket', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/notfound/authenticated/casestudy.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when uploading to duplicate object', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/public/sadcat-upload23.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 200 when uploading an object within bucket max size limit', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/public-limit-max-size-2/sadcat-upload25.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
  })

  test('return 400 when uploading an object that exceed bucket level max size', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/public-limit-max-size/sadcat-upload23.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(await response.json()).toEqual({
      error: 'Payload too large',
      message: 'The object exceeded the maximum allowed size',
      statusCode: '413',
    })
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
  })

  test('successfully uploading an object with a the allowed mime-type', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
      'content-type': 'image/jpeg',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/public-limit-mime-types/sadcat-upload23.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
  })

  test('successfully uploading an object with custom metadata using form data', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    form.append(
      'metadata',
      JSON.stringify({
        test1: 'test1',
        test2: 'test2',
      })
    )
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
      ...form.getHeaders(),
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/sadcat-upload3012.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()

    const client = await getSuperuserPostgrestClient()

    const object = await findObject(client, 'bucket2', 'sadcat-upload3012.png')

    expect(object).not.toBeFalsy()
    expect(object?.user_metadata).toEqual({
      test1: 'test1',
      test2: 'test2',
    })
  })

  test('successfully uploading an object with custom metadata using stream', async () => {
    const file = fs.createReadStream(`./src/test/assets/sadcat.jpg`)

    const headers = {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
      'x-metadata': Buffer.from(
        JSON.stringify({
          test1: 'test1',
          test2: 'test2',
        })
      ).toString('base64'),
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/sadcat-upload3018.png',
      headers,
      payload: file,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()

    const client = await getSuperuserPostgrestClient()

    const object = await findObject(client, 'bucket2', 'sadcat-upload3018.png')

    expect(object).not.toBeFalsy()
    expect(object?.user_metadata).toEqual({
      test1: 'test1',
      test2: 'test2',
    })
  })

  test('fetch object metadata', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    form.append(
      'metadata',
      JSON.stringify({
        test1: 'test1',
        test2: 'test2',
      })
    )
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
    })

    const uploadResponse = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/sadcat-upload3019.png',
      headers: {
        ...headers,
        ...form.getHeaders(),
      },
      payload: form,
    })
    expect(uploadResponse.statusCode).toBe(200)

    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/info/bucket2/sadcat-upload3019.png',
      headers,
    })

    const data = await response.json()

    expect(data.metadata).toEqual({
      test1: 'test1',
      test2: 'test2',
    })
  })

  test('can create an empty folder when mime-type is set', async () => {
    const form = new FormData()
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
    })

    form.append('file', Buffer.alloc(0))

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/public-limit-mime-types/nested/.emptyFolderPlaceholder',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
  })

  test('cannot create an empty folder with more than 0kb', async () => {
    const form = new FormData()
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
    })

    form.append('file', Buffer.alloc(1))

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/public-limit-mime-types/nested-2/.emptyFolderPlaceholder',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
  })

  test('return 400 when uploading an object with a not allowed mime-type (binary path)', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
      'content-type': 'image/png',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/public-limit-mime-types/sadcat-upload23.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(await response.json()).toEqual({
      error: 'invalid_mime_type',
      message: `mime type image/png is not supported`,
      statusCode: '415',
    })
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when uploading a multipart form-data object with a not allowed mime-type', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    form.append('contentType', 'image/png')
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/public-limit-mime-types/sadcat-upload23.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(await response.json()).toEqual({
      error: 'invalid_mime_type',
      message: `mime type image/png is not supported`,
      statusCode: '415',
    })
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('enforces allowed mime types set through bucket update', async () => {
    const bucketId = `allowed-mime-${randomUUID()}`
    const authHeader = { authorization: `Bearer ${await serviceKeyAsync}` }

    try {
      const createBucketResponse = await appInstance.inject({
        method: 'POST',
        url: '/bucket',
        headers: authHeader,
        payload: {
          name: bucketId,
        },
      })
      expect(createBucketResponse.statusCode).toBe(200)

      const updateBucketResponse = await appInstance.inject({
        method: 'PUT',
        url: `/bucket/${bucketId}`,
        headers: authHeader,
        payload: {
          allowed_mime_types: ['image/jpeg'],
        },
      })
      expect(updateBucketResponse.statusCode).toBe(200)

      const form = new FormData()
      form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
      form.append('contentType', 'image/png')

      const response = await appInstance.inject({
        method: 'POST',
        url: `/object/${bucketId}/sadcat-upload23.png`,
        headers: {
          ...form.getHeaders(),
          ...authHeader,
          'x-upsert': 'true',
        },
        payload: form,
      })

      expect(response.statusCode).toBe(400)
      expect(response.json()).toEqual({
        error: 'invalid_mime_type',
        message: `mime type image/png is not supported`,
        statusCode: '415',
      })
      expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
    } finally {
      const db = await getSuperuserPostgrestClient()
      await withDeleteEnabled(db, async (db) => {
        await db.query({
          text: 'DELETE FROM objects WHERE bucket_id = $1',
          values: [bucketId],
        })
        await db.query({
          text: 'DELETE FROM buckets WHERE id = $1',
          values: [bucketId],
        })
      })
    }
  })

  test('return 400 when uploading an object with a malformed mime-type', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
      'content-type': 'thisisnotarealmimetype',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/public-limit-mime-types/sadcat-upload23.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(await response.json()).toEqual({
      error: 'invalid_mime_type',
      message: 'Invalid Content-Type header',
      statusCode: '415',
    })
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when uploading an object with a content-type header containing tabs', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'x-upsert': 'true',
      'content-type': 'image/\tjpg',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/public-limit-mime-types/sadcat-upload23.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(await response.json()).toEqual({
      error: 'invalid_mime_type',
      message: 'Invalid Content-Type header',
      statusCode: '415',
    })
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 200 when upserting duplicate object', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
      'x-upsert': 'true',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/public/sadcat-upload23.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
  })

  test('return 400 when exceeding file size limit', async () => {
    mergeConfig({
      uploadFileSizeLimit: 1,
    })

    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
      // 'x-upsert': 'true',
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/public/sadcat55.jpg',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(response.body).toBe(
      JSON.stringify({
        statusCode: '413',
        error: 'Payload too large',
        message: 'The object exceeded the maximum allowed size',
      })
    )
  })

  test('return 400 when uploading to object with no file name', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
    })

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket4/',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('should not add row to database if upload fails', async () => {
    // Mock S3 upload failure.
    vi.spyOn(S3Backend.prototype, 'uploadObject').mockRejectedValue(
      StorageBackendError.fromError({
        name: 'S3ServiceException',
        message: 'Unknown error',
        $fault: 'server',
        $metadata: {
          httpStatusCode: 500,
        },
      })
    )

    process.env.FILE_SIZE_LIMIT = '1'
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
    })

    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'public/should-not-insert/sadcat.jpg'

    const createObjectResponse = await appInstance.inject({
      method: 'POST',
      url: `/object/${BUCKET_ID}/${OBJECT_NAME}`,
      headers,
      payload: form,
    })
    expect(createObjectResponse.statusCode).toBe(500)
    expect(JSON.parse(createObjectResponse.body)).toStrictEqual({
      code: ErrorCode.S3Error,
      statusCode: '500',
      error: 'Unknown error',
      message: 'S3ServiceException',
    })

    // Ensure that row does not exist in database.
    const db = await getSuperuserPostgrestClient()
    const objectResponse = await findObject(db, BUCKET_ID, OBJECT_NAME)

    expect(objectResponse).toBe(undefined)
  })
})

/*
 * POST /object/:id
 * binary upload
 */
describe('testing POST object via binary upload', () => {
  test('check if RLS policies are respected: authenticated user is able to upload authenticated resource', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
      'x-upsert': 'true',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/authenticated/binary-casestudy1.png',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
    expect(await response.json()).toEqual(
      expect.objectContaining({
        Id: expect.any(String),
        Key: 'bucket2/authenticated/binary-casestudy1.png',
      })
    )
  })

  test('check if RLS policies are respected: anon user is not able to upload authenticated resource', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${anonKey}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/authenticated/binary-casestudy.png',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
    expect(response.body).toBe(
      JSON.stringify({
        statusCode: '403',
        error: 'Unauthorized',
        message: 'new row violates row-level security policy',
      })
    )
  })

  test('check if RLS policies are respected: user is not able to upload a resource without Auth header', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/authenticated/binary-casestudy1.png',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when uploading to a non existent bucket', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/notfound/authenticated/binary-casestudy1.png',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when uploading to duplicate object', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/public/sadcat-upload23.png',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 200 when upserting duplicate object', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
      'x-upsert': 'true',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/public/sadcat-upload23.png',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
  })

  test('return 400 when exceeding file size limit', async () => {
    mergeConfig({
      uploadFileSizeLimit: 1,
    })
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket2/public/sadcat.jpg',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(response.body).toBe(
      JSON.stringify({
        statusCode: '413',
        error: 'Payload too large',
        message: 'The object exceeded the maximum allowed size',
      })
    )
  })

  test('return 400 when a binary upload spoofs x-amz-decoded-content-length', async () => {
    mergeConfig({
      uploadFileSizeLimit: 1,
    })

    const bucketId = `spoof-decoded-${randomUUID()}`
    const superUser = await getServiceKeyUser(tenantId)
    const db = await getPostgresConnection({
      superUser,
      user: superUser,
      tenantId,
      host: 'localhost',
    })
    const setupTx = await db.transaction()
    await insertBucket(setupTx, {
      id: bucketId,
      name: bucketId,
      public: true,
      file_size_limit: null,
      allowed_mime_types: null,
      type: 'STANDARD',
    })
    await setupTx.commit()
    db.dispose()

    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${await serviceKeyAsync}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
      'x-amz-decoded-content-length': '1',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/${bucketId}/public/sadcat-spoofed-decoded-length.jpg`,
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(response.body).toBe(
      JSON.stringify({
        statusCode: '413',
        error: 'Payload too large',
        message: 'The object exceeded the maximum allowed size',
      })
    )
    // Early size check in fileUploadFromRequest rejects before reaching the backend
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when uploading to object with no file name', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${anonKey}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
      'x-upsert': 'true',
    }

    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/bucket4/',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('should not add row to database if upload fails', async () => {
    // Mock S3 upload failure.
    vi.spyOn(S3Backend.prototype, 'uploadObject').mockRejectedValue(
      StorageBackendError.fromError({
        name: 'S3ServiceException',
        message: 'Unknown error',
        $fault: 'server',
        $metadata: {
          httpStatusCode: 500,
        },
      })
    )

    process.env.FILE_SIZE_LIMIT = '1'
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'public/should-not-insert/sadcat.jpg'

    const createObjectResponse = await appInstance.inject({
      method: 'POST',
      url: `/object/${BUCKET_ID}/${OBJECT_NAME}`,
      headers,
      payload: fs.createReadStream(path),
    })
    expect(createObjectResponse.statusCode).toBe(500)
    expect(JSON.parse(createObjectResponse.body)).toStrictEqual({
      statusCode: '500',
      code: ErrorCode.S3Error,
      error: 'Unknown error',
      message: 'S3ServiceException',
    })

    // Ensure that row does not exist in database.
    const db = await getSuperuserPostgrestClient()
    const objectResponse = await findObject(db, BUCKET_ID, OBJECT_NAME)
    expect(objectResponse).toBe(undefined)
  })
})

/**
 * PUT /object/:id
 * multipart upload
 */
describe('testing PUT object', () => {
  test('check if RLS policies are respected: authenticated user is able to update authenticated resource', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
    })

    const response = await appInstance.inject({
      method: 'PUT',
      url: '/object/bucket2/authenticated/cat.jpg',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
    expect(await response.json()).toEqual(
      expect.objectContaining({
        Id: expect.any(String),
        Key: 'bucket2/authenticated/cat.jpg',
      })
    )
  })

  test('check if RLS policies are respected: anon user is not able to update authenticated resource', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
    })

    const response = await appInstance.inject({
      method: 'PUT',
      url: '/object/bucket2/authenticated/cat.jpg',
      headers,
      payload: form,
    })

    expect(response.statusCode).toBe(400)

    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('user is not able to update a resource without Auth header', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))

    const response = await appInstance.inject({
      method: 'PUT',
      url: '/object/bucket2/authenticated/cat.jpg',
      headers: form.getHeaders(),
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when update to a non existent bucket', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
    })

    const response = await appInstance.inject({
      method: 'PUT',
      url: '/object/notfound/authenticated/cat.jpg',
      headers,
      payload: form,
    })

    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when updating a non existent key', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
    })

    const response = await appInstance.inject({
      method: 'PUT',
      url: '/object/notfound/authenticated/notfound.jpg',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })
})

/*
 * PUT /object/:id
 * binary upload
 */
describe('testing PUT object via binary upload', () => {
  test('check if RLS policies are respected: authenticated user is able to update authenticated resource', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await appInstance.inject({
      method: 'PUT',
      url: '/object/bucket2/authenticated/cat.jpg',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
    expect(await response.json()).toEqual(
      expect.objectContaining({
        Id: expect.any(String),
        Key: 'bucket2/authenticated/cat.jpg',
      })
    )
  })

  test('replaces custom metadata when updating an object', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)
    const objectName = `metadata-replace/${randomUUID()}.jpg`
    const initialMetadata = { keep: false, stale: 'removed' }
    const replacementMetadata = { keep: true, fresh: 'present' }

    try {
      const createResponse = await appInstance.inject({
        method: 'POST',
        url: `/object/bucket2/${objectName}`,
        headers: {
          authorization: `Bearer ${await serviceKeyAsync}`,
          'Content-Length': size,
          'Content-Type': 'image/jpeg',
          'x-metadata': Buffer.from(JSON.stringify(initialMetadata)).toString('base64'),
        },
        payload: fs.createReadStream(path),
      })
      expect(createResponse.statusCode).toBe(200)

      const updateResponse = await appInstance.inject({
        method: 'PUT',
        url: `/object/bucket2/${objectName}`,
        headers: {
          authorization: `Bearer ${await serviceKeyAsync}`,
          'Content-Length': size,
          'Content-Type': 'image/jpeg',
          'x-metadata': Buffer.from(JSON.stringify(replacementMetadata)).toString('base64'),
        },
        payload: fs.createReadStream(path),
      })
      expect(updateResponse.statusCode).toBe(200)

      const infoResponse = await appInstance.inject({
        method: 'GET',
        url: `/object/info/bucket2/${objectName}`,
        headers: {
          authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })
      expect(infoResponse.statusCode).toBe(200)
      expect(infoResponse.json().metadata).toEqual(replacementMetadata)
    } finally {
      const db = await getSuperuserPostgrestClient()
      await withDeleteEnabled(db, async (db) => {
        await db.query({
          text: `
            DELETE FROM objects
            WHERE name = $1
              AND bucket_id = $2
          `,
          values: [objectName, 'bucket2'],
        })
      })
    }
  })

  test('check if RLS policies are respected: anon user is not able to update authenticated resource', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${anonKey}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await appInstance.inject({
      method: 'PUT',
      url: '/object/bucket2/authenticated/cat.jpg',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('check if RLS policies are respected: user is not able to upload a resource without Auth header', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await appInstance.inject({
      method: 'PUT',
      url: '/object/bucket2/authenticated/cat.jpg',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when updating an object in a non existent bucket', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await appInstance.inject({
      method: 'PUT',
      url: '/object/notfound/authenticated/binary-casestudy1.png',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('return 400 when updating an object in a non existent key', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await appInstance.inject({
      method: 'PUT',
      url: '/object/notfound/authenticated/notfound.jpg',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })
})

/**
 * POST /copy
 */
describe('testing copy object', () => {
  test('check if RLS policies are respected: authenticated user is able to copy authenticated resource', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        bucketId: 'bucket2',
        sourceKey: 'authenticated/casestudy.png',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.copyObject).toHaveBeenCalled()
    const jsonResponse = await response.json()
    expect(jsonResponse.Key).toBe(`bucket2/authenticated/casestudy11.png`)
  })

  test('can copy objects across buckets', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        bucketId: 'bucket2',
        sourceKey: 'authenticated/casestudy.png',
        destinationBucket: 'bucket3',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.copyObject).toHaveBeenCalled()
    const jsonResponse = await response.json()

    expect(jsonResponse.Key).toBe(`bucket3/authenticated/casestudy11.png`)
  })

  test('can copy objects keeping their metadata', async () => {
    const copiedKey = 'casestudy-2349.png'
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        bucketId: 'bucket2',
        sourceKey: 'authenticated/casestudy.png',
        destinationKey: `authenticated/${copiedKey}`,
        copyMetadata: true,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.copyObject).toHaveBeenCalled()
    const jsonResponse = response.json()
    expect(jsonResponse.Key).toBe(`bucket2/authenticated/${copiedKey}`)

    const conn = await getSuperuserPostgrestClient()
    const object = await findObject(conn, 'bucket2', `authenticated/${copiedKey}`)

    expect(object).not.toBeFalsy()
    expect(object!.user_metadata).toEqual({
      test1: 1234,
    })
  })

  test('can copy objects to itself overwriting their metadata', async () => {
    const copiedKey = 'casestudy-2349.png'
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        'x-upsert': 'true',
        'x-metadata': Buffer.from(
          JSON.stringify({
            newMetadata: 'test1',
          })
        ).toString('base64'),
      },
      payload: {
        bucketId: 'bucket2',
        sourceKey: `authenticated/${copiedKey}`,
        destinationKey: `authenticated/${copiedKey}`,
        metadata: {
          cacheControl: 'max-age=999',
          mimetype: 'image/gif',
        },
        copyMetadata: false,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.copyObject).toHaveBeenCalled()
    const parsedBody = JSON.parse(response.body)

    expect(parsedBody.Key).toBe(`bucket2/authenticated/${copiedKey}`)
    expect(parsedBody.name).toBe(`authenticated/${copiedKey}`)
    expect(parsedBody.bucket_id).toBe(`bucket2`)
    expect(parsedBody.metadata).toEqual(
      expect.objectContaining({
        cacheControl: 'max-age=999',
        mimetype: 'image/gif',
      })
    )

    const conn = await getSuperuserPostgrestClient()
    const object = await findObject(conn, 'bucket2', `authenticated/${copiedKey}`)

    expect(object).not.toBeFalsy()
    expect(object!.user_metadata).toEqual({
      newMetadata: 'test1',
    })
    expect(object!.metadata).toEqual(
      expect.objectContaining({
        cacheControl: 'max-age=999',
        mimetype: 'image/gif',
      })
    )
  })

  test('can copy objects excluding their metadata', async () => {
    const copiedKey = 'casestudy-2450.png'
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        bucketId: 'bucket2',
        sourceKey: 'authenticated/casestudy.png',
        destinationKey: `authenticated/${copiedKey}`,
        copyMetadata: false,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.copyObject).toHaveBeenCalled()
    const jsonResponse = response.json()
    expect(jsonResponse.Key).toBe(`bucket2/authenticated/${copiedKey}`)

    const conn = await getSuperuserPostgrestClient()
    const object = await findObject(conn, 'bucket2', `authenticated/${copiedKey}`)

    expect(object).not.toBeFalsy()
    expect(object!.user_metadata).toBeNull()
  })

  test('cannot copy objects across buckets when RLS dont allow it', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        bucketId: 'bucket2',
        sourceKey: 'authenticated/casestudy.png',
        destinationBucket: 'bucket3',
        destinationKey: 'somekey/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('check if RLS policies are respected: anon user is not able to update authenticated resource', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        bucketId: 'bucket2',
        sourceKey: 'authenticated/casestudy.png',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.copyObject).not.toHaveBeenCalled()
  })

  test('user is not able to copy a resource without Auth header', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/copy',
      payload: {
        bucketId: 'bucket2',
        sourceKey: 'authenticated/casestudy.png',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.copyObject).not.toHaveBeenCalled()
  })

  test('return 400 when copy from a non existent bucket', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        bucketId: 'notfound',
        sourceKey: 'authenticated/casestudy.png',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.copyObject).not.toHaveBeenCalled()
  })

  test('return 400 when copying a non existent key', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        bucketId: 'bucket2',
        sourceKey: 'authenticated/notfound.png',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.copyObject).not.toHaveBeenCalled()
  })
})

/**
 * DELETE /object
 * */
describe('testing delete object', () => {
  test('check if RLS policies are respected: authenticated user is able to delete authenticated resource', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/bucket2/authenticated/delete.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.deleteObject).toHaveBeenCalled()
  })

  test('check if RLS policies are respected: anon user is not able to delete authenticated resource', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/bucket2/authenticated/delete1.png',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.deleteObject).not.toHaveBeenCalled()
  })

  test('user is not able to delete a resource without Auth header', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/bucket2/authenticated/delete1.png',
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.deleteObject).not.toHaveBeenCalled()
  })

  test('return 400 when delete from a non existent bucket', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/notfound/authenticated/delete1.png',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.deleteObject).not.toHaveBeenCalled()
  })

  test('return 400 when deleting a non existent key', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/notfound/authenticated/notfound.jpg',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.deleteObject).not.toHaveBeenCalled()
  })
})

/**
 * DELETE /objects
 * */
describe('testing deleting multiple objects', () => {
  test('authenticated user can bulk delete objects up to the request cap', async () => {
    const runId = randomUUID()
    const bucketName = 'bucket2'
    const objectNames = [...Array(MAX_OBJECTS_PER_REQUEST).keys()].map(
      (i) => `authenticated/bulk-delete-${runId}/${i}`
    )

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjectNames(seedTx, bucketName, objectNames)
    await seedTx.commit()
    tnx = undefined

    try {
      const response = await appInstance.inject({
        method: 'DELETE',
        url: `/object/${bucketName}`,
        headers: {
          authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        },
        payload: {
          prefixes: objectNames,
        },
      })
      expect(response.statusCode).toBe(200)
      expect(S3Backend.prototype.deleteObjects).toHaveBeenCalled()

      const result = JSON.parse(response.body)
      expect(result).toHaveLength(MAX_OBJECTS_PER_REQUEST)
      expect(result.map((row: { name: string }) => row.name)).toEqual(
        expect.arrayContaining(objectNames)
      )
    } finally {
      const cleanupTx = await getSuperuserPostgrestClient()
      await withDeleteEnabled(cleanupTx, async (db) => {
        await deleteObjectsByName(db, bucketName, objectNames)
      })
      await cleanupTx.commit()
      tnx = undefined
    }
  })

  test('allows delete requests over the object request cap when hard limits are disabled', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/bucket2',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        prefixes: [...Array(MAX_OBJECTS_PER_REQUEST + 1).keys()].map(
          (i) => `authenticated/too-many-${i}`
        ),
      },
    })

    expect(response.statusCode).toBe(200)
    expect(JSON.parse(response.body)).toEqual([])
    expect(S3Backend.prototype.deleteObjects).not.toHaveBeenCalled()
  })

  test('check if RLS policies are respected: anon user is not able to delete authenticated resource', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/bucket2',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        prefixes: ['authenticated/delete-multiple3.png', 'authenticated/delete-multiple4.png'],
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.deleteObjects).not.toHaveBeenCalled()
    const results = JSON.parse(response.body)
    expect(results).toHaveLength(0)
  })

  test('user is not able to delete a resource without Auth header', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/bucket2',
      payload: {
        prefixes: ['authenticated/delete-multiple3.png', 'authenticated/delete-multiple4.png'],
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.deleteObjects).not.toHaveBeenCalled()
  })

  test('deleting from a non existent bucket', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/notfound',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        prefixes: ['authenticated/delete-multiple3.png', 'authenticated/delete-multiple4.png'],
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.deleteObjects).not.toHaveBeenCalled()
  })

  test('deleting a non existent key', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/bucket2',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        prefixes: ['authenticated/delete-multiple5.png', 'authenticated/delete-multiple6.png'],
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.deleteObjects).not.toHaveBeenCalled()
    const results = JSON.parse(response.body)
    expect(results).toHaveLength(0)
  })

  test('check if RLS policies are respected: user has permission to delete only one of the objects', async () => {
    const response = await appInstance.inject({
      method: 'DELETE',
      url: '/object/bucket2',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        prefixes: ['authenticated/delete-multiple7.png', 'private/sadcat-upload3.png'],
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.deleteObjects).toHaveBeenCalled()
    const results = JSON.parse(response.body)
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('authenticated/delete-multiple7.png')
  })
})

/**
 * POST /sign/:bucketName/*
 */
describe('testing generating signed URL', () => {
  test('check if RLS policies are respected: authenticated user is able to sign URL for an authenticated resource', async () => {
    const assetUrl = 'bucket2/authenticated/cat.jpg'
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/' + assetUrl,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1000,
      },
    })
    expect(response.statusCode).toBe(200)
    const result = JSON.parse(response.body)
    expect(result.signedURL).toBeTruthy()
    expect(result.signedURL).toContain('?token=')

    // verify was correctly signed with jwtSecret
    const token = result.signedURL.split('?token=').pop()
    const jwtData = (await verifyJWT(token, jwtSecret)) as SignedToken
    expect(jwtData.url).toBe(assetUrl)
  })

  test('check if url signing key is used to sign urls (instead of jwtSecret) if it is present', async () => {
    const signingJwk = { ...(await generateHS512JWK()), kid: 'qwerty-09876' } as JwksConfigKeyOCT
    const jwtJWKS: JwksConfig = { keys: [signingJwk], urlSigningKey: signingJwk }
    mergeConfig({ jwtJWKS })

    const assetUrl = 'bucket2/authenticated/cat.jpg'
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/' + assetUrl,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1000,
      },
    })
    expect(response.statusCode).toBe(200)
    const result = JSON.parse(response.body)
    expect(result.signedURL).toBeTruthy()
    expect(result.signedURL).toContain('?token=')

    // verify was correctly signed with url signing key (jwk)
    const token = result.signedURL.split('?token=').pop()
    const jwtData = (await verifyJWT(token, 'invalid-old-jwt-secret', jwtJWKS)) as SignedToken
    expect(jwtData.url).toBe(assetUrl)
  })

  test('check if RLS policies are respected: anon user is not able to generate signedURL for authenticated resource', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2/authenticated/cat.jpg',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        expiresIn: 1000,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to generate signedURLs without Auth header', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2/authenticated/cat.jpg',
      payload: {
        expiresIn: 1000,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('return 400 when generate signed urls from a non existent bucket', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/notfound/authenticated/cat.jpg',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1000,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('signing url of a non existent key', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2/authenticated/notfound.jpg',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1000,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('rejects oversized expiresIn values for signed URLs before jwt signing', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2/authenticated/cat.jpg',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1e21,
      },
    })

    expect(response.statusCode).toBe(400)
    expect(JSON.parse(response.body).message).toContain('expiresIn')
  })

  test('rejects expiresIn values above the current runtime maximum for signed URLs', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2/authenticated/cat.jpg',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: getMaxNumericJWTExpiration() + 10,
      },
    })

    expect(response.statusCode).toBe(400)
    expect(JSON.parse(response.body).message).toContain('expiresIn')
  })
})

/**
 * POST /upload/sign/:bucketName/*
 */
describe('testing generating signed URL for upload', () => {
  test('check if RLS policies are respected: authenticated user is able to sign upload URL for a resource', async () => {
    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'authenticated/cat1.jpg'

    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/upload/sign/${BUCKET_ID}/${OBJECT_NAME}`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const result = JSON.parse(response.body)
    expect(result.url).toBeTruthy()
    // Ensure that row does not exist in database.
    const db = await getSuperuserPostgrestClient()
    const objectResponse = await findObject(db, BUCKET_ID, OBJECT_NAME)
    expect(objectResponse).toBe(undefined)
  })

  test('check if RLS policies are respected: anon user is not able to sign upload URL for authenticated resource', async () => {
    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'authenticated/cat1.jpg'

    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/upload/sign/${BUCKET_ID}/${OBJECT_NAME}`,
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(response.body).toBe(
      JSON.stringify({
        statusCode: '403',
        error: 'Unauthorized',
        message: 'new row violates row-level security policy',
      })
    )
    // Ensure that row does not exist in database.
    const db = await getSuperuserPostgrestClient()
    const objectResponse = await findObject(db, BUCKET_ID, OBJECT_NAME)
    expect(objectResponse).toBe(undefined)
  })

  test('user is not able to sign a upload url without Auth header', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/upload/sign/bucket2/authenticated/cat.jpg',
    })
    expect(response.statusCode).toBe(400)
  })

  test('return 400 when generating signed upload urls from a non existent bucket', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/upload/sign/notfound/authenticated/cat.jpg',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('signing upload url of a non existent key', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/upload/sign/bucket2/authenticated/notfound.jpg',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
  })

  test('signing upload url of an existent key', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/upload/sign/bucket2/authenticated/cat.jpg',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(JSON.parse(response.body).statusCode).toBe('409')
  })
})

/**
 * PUT /upload/sign/:bucketName/*
 */
describe('testing uploading with generated signed upload URL', () => {
  test('upload object with a token', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      'content-type': 'image/jpeg',
    })

    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'public/sadcat-upload1.png'
    const urlToSign = `${BUCKET_ID}/${OBJECT_NAME}`
    const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'

    const jwtToken = await signJWT(
      { owner, url: urlToSign, scope: SIGNED_URL_SCOPE_UPLOAD },
      jwtSecret,
      100
    )
    const response = await appInstance.inject({
      method: 'PUT',
      url: `/object/upload/sign/${urlToSign}?token=${jwtToken}`,
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()

    // check that row has neccessary data
    const db = await getSuperuserPostgrestClient()
    const objectResponse = await findObject(db, BUCKET_ID, OBJECT_NAME)
    expect(objectResponse?.owner).toBe(owner)

    // remove row to not to break other tests
    await withDeleteEnabled(db, async (db) => {
      await deleteObjectsByName(db, BUCKET_ID, OBJECT_NAME)
    })
  })

  test('upload object without a token', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      'content-type': 'image/jpeg',
    })

    const response = await appInstance.inject({
      method: 'PUT',
      url: `/object/upload/sign/bucket2/public/sadcat-upload1.png`,
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('upload object with a malformed JWT', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      'content-type': 'image/jpeg',
    })

    const response = await appInstance.inject({
      method: 'PUT',
      url: `/object/upload/sign/bucket2/public/sadcat-upload1.png?token=xxx`,
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('rejects a download-scoped token on the upload endpoint', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      'content-type': 'image/jpeg',
    })

    const urlToSign = `bucket2/public/sadcat-upload1.png`
    // A token minted by the download-signing flow must not be replayable to upload
    const downloadToken = await signJWT(
      { url: urlToSign, scope: SIGNED_URL_SCOPE_DOWNLOAD },
      jwtSecret,
      100
    )

    const response = await appInstance.inject({
      method: 'PUT',
      url: `/object/upload/sign/${urlToSign}?token=${downloadToken}`,
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(response.json()).toMatchObject({ error: ErrorCode.InvalidSignature })
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('rejects a legacy download-shaped token (no upsert) on the upload endpoint', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      'content-type': 'image/jpeg',
    })

    const urlToSign = `bucket2/public/sadcat-upload1.png`
    const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'
    // No scope claim and no `upsert` claim — i.e. a download-shaped token — is rejected,
    // even though it predates scoping. Only legacy *upload* tokens (with upsert) are honored.
    const unscopedToken = await signJWT({ owner, url: urlToSign }, jwtSecret, 100)

    const response = await appInstance.inject({
      method: 'PUT',
      url: `/object/upload/sign/${urlToSign}?token=${unscopedToken}`,
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(response.json()).toMatchObject({ error: ErrorCode.InvalidSignature })
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('accepts a legacy upload token (no scope, with upsert) for backward compatibility', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      'content-type': 'image/jpeg',
    })

    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'public/sadcat-legacy-upload.png'
    const urlToSign = `${BUCKET_ID}/${OBJECT_NAME}`
    const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'
    // Token shaped exactly like one minted before scoping existed: owner + url + upsert, no scope
    const legacyUploadToken = await signJWT(
      { owner, url: urlToSign, upsert: false },
      jwtSecret,
      100
    )

    const response = await appInstance.inject({
      method: 'PUT',
      url: `/object/upload/sign/${urlToSign}?token=${legacyUploadToken}`,
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()

    // cleanup so the test can be re-run against the same dataset
    const db = await getSuperuserPostgrestClient()
    await withDeleteEnabled(db, async (db) => {
      await deleteObjectsByName(db, BUCKET_ID, OBJECT_NAME)
    })
  })

  test('upload object with an expired JWT', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      'content-type': 'image/jpeg',
    })

    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'public/sadcat-upload1.png'
    const urlToSign = `${BUCKET_ID}/${OBJECT_NAME}`
    const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'

    const jwtToken = await signJWT(
      { owner, url: urlToSign, scope: SIGNED_URL_SCOPE_UPLOAD },
      jwtSecret,
      '-1s'
    )
    const response = await appInstance.inject({
      method: 'PUT',
      url: `/object/upload/sign/${urlToSign}?token=${jwtToken}`,
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  test('upload object with a tampered signed upload token', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      'content-type': 'image/jpeg',
    })

    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'public/sadcat-upload1.png'
    const urlToSign = `${BUCKET_ID}/${OBJECT_NAME}`
    const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'
    const jwtToken = await signJWT(
      { owner, url: urlToSign, scope: SIGNED_URL_SCOPE_UPLOAD },
      jwtSecret,
      100
    )
    const signatureStart = jwtToken.lastIndexOf('.') + 1
    const signatureChar = jwtToken[signatureStart]
    const tamperedToken = `${jwtToken.slice(0, signatureStart)}${
      signatureChar === 'a' ? 'b' : 'a'
    }${jwtToken.slice(signatureStart + 1)}`

    const response = await appInstance.inject({
      method: 'PUT',
      url: `/object/upload/sign/${urlToSign}?token=${tamperedToken}`,
      headers,
      payload: form,
    })

    expect(response.statusCode).toBe(400)
    expect(response.json()).toMatchObject({
      statusCode: '400',
      error: ErrorCode.InvalidJWT,
    })
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
  })

  it('will allow overwriting a file when the generating a signed upload url with x-upsert:true', async () => {
    function createUpload() {
      const form = new FormData()
      form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
      return form
    }

    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'signed/sadcat-upload-signed-2.png'
    const urlToSign = `${BUCKET_ID}/${OBJECT_NAME}`

    // Upload a file first
    const resp = await appInstance.inject({
      method: 'POST',
      url: `/object/${urlToSign}`,
      payload: createUpload(),
      headers: {
        'x-upsert': 'true',
        authorization: await serviceKeyAsync,
      },
    })

    expect(resp.statusCode).toBe(200)

    // generate signed upload url with upsert
    const signedUrlResp = await appInstance.inject({
      method: 'POST',
      url: `/object/upload/sign/${urlToSign}`,
      headers: {
        'x-upsert': 'true',
        authorization: await serviceKeyAsync,
      },
    })
    expect(signedUrlResp.statusCode).toBe(200)

    const jwtToken = (await signedUrlResp.json()).token
    const response = await appInstance.inject({
      method: 'PUT',
      url: `/object/upload/sign/${urlToSign}?token=${jwtToken}`,
      payload: createUpload(),
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
  })

  it('will allow not be able overwriting a file when the generating a signed upload url without x-upsert header', async () => {
    function createUpload() {
      const form = new FormData()
      form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
      return form
    }

    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'signed/sadcat-upload-signed-3.png'
    const urlToSign = `${BUCKET_ID}/${OBJECT_NAME}`
    const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'

    // Upload a file first
    const resp = await appInstance.inject({
      method: 'POST',
      url: `/object/${urlToSign}`,
      payload: createUpload(),
      headers: {
        authorization: await serviceKeyAsync,
      },
    })

    expect(resp.statusCode).toBe(200)

    const jwtToken = await signJWT(
      { owner, url: urlToSign, scope: SIGNED_URL_SCOPE_UPLOAD },
      jwtSecret,
      100
    )
    const response = await appInstance.inject({
      method: 'PUT',
      url: `/object/upload/sign/${urlToSign}?token=${jwtToken}`,
      payload: createUpload(),
    })
    expect(response.statusCode).toBe(400)
  })
})

/**
 * POST /sign/:bucketName
 */
describe('testing generating signed URLs', () => {
  test('check if RLS policies are respected: authenticated user is able to sign URLs for an authenticated resource', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1000,
        paths: [...Array(MAX_OBJECTS_PER_REQUEST).keys()].map((i) => `authenticated/${i}`),
      },
    })
    expect(response.statusCode).toBe(200)
    const result = JSON.parse(response.body)
    expect(result).toHaveLength(MAX_OBJECTS_PER_REQUEST)
  })

  test('authenticated user can sign URLs up to the request cap', async () => {
    const runId = randomUUID()
    const bucketName = 'bucket2'
    const objectNames = [...Array(MAX_OBJECTS_PER_REQUEST).keys()].map(
      (i) => `authenticated/bulk-sign-${runId}/${i}`
    )

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjectNames(seedTx, bucketName, objectNames)
    await seedTx.commit()
    tnx = undefined

    try {
      const response = await appInstance.inject({
        method: 'POST',
        url: `/object/sign/${bucketName}`,
        headers: {
          authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        },
        payload: {
          expiresIn: 1000,
          paths: objectNames,
        },
      })
      expect(response.statusCode).toBe(200)
      const result = JSON.parse(response.body) as SignedUrlResult[]
      expect(result).toHaveLength(MAX_OBJECTS_PER_REQUEST)
      expect(
        result.every(({ error, signedURL }) => {
          return error === null && signedURL !== null
        })
      ).toBe(true)
    } finally {
      const cleanupTx = await getSuperuserPostgrestClient()
      await withDeleteEnabled(cleanupTx, async (db) => {
        await deleteObjectsByName(db, bucketName, objectNames)
      })
      await cleanupTx.commit()
      tnx = undefined
    }
  })

  test('check if RLS policies are respected: anon user is not able to generate signedURLs for authenticated resource', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        expiresIn: 1000,
        paths: [...Array(MAX_OBJECTS_PER_REQUEST).keys()].map((i) => `authenticated/${i}`),
      },
    })
    expect(response.statusCode).toBe(200)
    const result = JSON.parse(response.body)
    expect(result[0].error).toBe('Either the object does not exist or you do not have access to it')
  })

  test('user is not able to generate signedURLs without Auth header', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2',
      payload: {
        expiresIn: 1000,
        paths: [...Array(MAX_OBJECTS_PER_REQUEST).keys()].map((i) => `authenticated/${i}`),
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('rejects signed URL requests over the object request cap', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1000,
        paths: [...Array(MAX_OBJECTS_PER_REQUEST + 1).keys()].map(
          (i) => `authenticated/too-many-${i}`
        ),
      },
    })

    expect(response.statusCode).toBe(400)
  })

  test('return 400 when generate signed urls from a non existent bucket', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/notfound',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1000,
        paths: [...Array(MAX_OBJECTS_PER_REQUEST).keys()].map((i) => `authenticated/${i}`),
      },
    })
    expect(response.statusCode).toBe(200)
    const result = JSON.parse(response.body)
    expect(result[0].error).toBe('Either the object does not exist or you do not have access to it')
  })

  test('signing url of a non existent key', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2clearAllMocks',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1000,
        paths: ['authenticated/notfound.jpg'],
      },
    })
    expect(response.statusCode).toBe(200)
    const result = JSON.parse(response.body)
    expect(result[0].error).toBe('Either the object does not exist or you do not have access to it')
  })

  test('rejects oversized expiresIn values for batch signed URLs before jwt signing', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/sign/bucket2',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1e21,
        paths: ['authenticated/cat.jpg'],
      },
    })

    expect(response.statusCode).toBe(400)
    expect(JSON.parse(response.body).message).toContain('expiresIn')
  })
})

/**
 * GET /public/
 */
// these tests are written in bucket.test.ts since its easier

/**
 * signObjectUrl payload hardening (signing-oracle defense)
 */
describe('signObjectUrl token claim hardening', () => {
  const h = useStorage()

  test('attacker-controlled metadata cannot override url/scope or inject upload claims', async () => {
    const objectName = 'public/sadcat-upload.png'
    const signedURL = await h.storage
      .from('bucket2')
      .signObjectUrl(objectName, `/object/sign/bucket2/${objectName}`, 100, {
        // a future caller passing these must never be able to forge the token
        url: 'other-bucket/secret.png',
        scope: SIGNED_URL_SCOPE_UPLOAD,
        role: 'service_role',
        upsert: true,
        owner: 'attacker',
      } as never)

    const token = signedURL.split('?token=').pop() as string
    const payload = (await verifyJWT(token, jwtSecret)) as Record<string, unknown>

    // url stays pinned to the real object path, scope stays 'download'
    expect(payload.url).toBe(`bucket2/${objectName}`)
    expect(payload.scope).toBe(SIGNED_URL_SCOPE_DOWNLOAD)
    // role and the upload-discriminating claims are stripped entirely
    expect(payload.role).toBeUndefined()
    expect(payload.upsert).toBeUndefined()
    expect(payload.owner).toBeUndefined()
  })
})

/**
 * GET /sign/
 */
describe('testing retrieving signed URL', () => {
  test('get object with a token', async () => {
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const jwtToken = await signJWT({ url: urlToSign }, jwtSecret, 100)
    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${jwtToken}`,
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['x-robots-tag']).toBe('none')
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['last-modified']).toBe('Thu, 12 Aug 2021 16:00:00 GMT')
  })

  test('get object with jwk generated token', async () => {
    const signingJwk = { ...(await generateHS512JWK()), kid: 'abc-123' } as JwksConfigKeyOCT
    mergeConfig({ jwtJWKS: { keys: [signingJwk] } })

    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const jwtToken = await signJWT({ url: urlToSign }, signingJwk, 100)
    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${jwtToken}`,
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['last-modified']).toBe('Thu, 12 Aug 2021 16:00:00 GMT')
  })

  test('forward 304 and If-Modified-Since/If-None-Match headers', async () => {
    const mockGetObject = vi.spyOn(S3Backend.prototype, 'getObject')
    mockGetObject.mockRejectedValue({
      $metadata: {
        httpStatusCode: 304,
      },
    })
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const jwtToken = await signJWT({ url: urlToSign }, jwtSecret, 100)
    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${jwtToken}`,
      headers: {
        'if-modified-since': 'Thu, 12 Aug 2021 16:00:00 GMT',
        'if-none-match': 'abc',
      },
    })
    expect(response.statusCode).toBe(304)
    expect(mockGetObject.mock.calls[0][3]).toMatchObject({
      ifModifiedSince: 'Thu, 12 Aug 2021 16:00:00 GMT',
      ifNoneMatch: 'abc',
    })
  })

  test('rejects an upload-scoped token on the download endpoint', async () => {
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'
    // A token minted by the upload-signing flow must not be replayable to download
    const uploadToken = await signJWT(
      { owner, url: urlToSign, upsert: false, scope: SIGNED_URL_SCOPE_UPLOAD },
      jwtSecret,
      100
    )
    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${uploadToken}`,
    })
    expect(response.statusCode).toBe(400)
    expect(response.json<{ error: string }>().error).toBe('InvalidSignature')
  })

  test('still serves a legacy unscoped download token', async () => {
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    // Tokens issued before scoping existed (no scope claim, no upsert) remain valid for download
    const legacyToken = await signJWT({ url: urlToSign }, jwtSecret, 100)
    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${legacyToken}`,
    })
    expect(response.statusCode).toBe(200)
  })

  test('rejects a legacy upload-shaped token (with upsert) on the download endpoint', async () => {
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const owner = '317eadce-631a-4429-a0bb-f19a7a517b4a'
    // A legacy upload token (no scope, but carrying upsert) must not be replayable to read
    const legacyUploadToken = await signJWT(
      { owner, url: urlToSign, upsert: false },
      jwtSecret,
      100
    )
    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${legacyUploadToken}`,
    })
    expect(response.statusCode).toBe(400)
    expect(response.json<{ error: string }>().error).toBe('InvalidSignature')
  })

  test('get object with incorrect url in jwt', async () => {
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const jwtToken = await signJWT({ url: 'some/other/weird-path.png' }, jwtSecret, 100)
    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${jwtToken}`,
    })
    expect(response.statusCode).toBe(400)
    const body = response.json<{ error: string }>()
    expect(body.error).toBe('InvalidSignature')
  })

  test('get object without a token', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/sign/bucket2/public/sadcat-upload.png',
    })
    expect(response.statusCode).toBe(400)
  })

  test('get object with a malformed JWT', async () => {
    const response = await appInstance.inject({
      method: 'GET',
      url: '/object/sign/bucket2/public/sadcat-upload.png?token=xxx',
    })
    expect(response.statusCode).toBe(400)
  })

  test('get object with an expired JWT', async () => {
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const expiredJWT = await signJWT({ url: urlToSign }, jwtSecret, '-1s')
    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${expiredJWT}`,
    })
    expect(response.statusCode).toBe(400)
  })
})

describe('testing move object', () => {
  test('check if RLS policies are respected: authenticated user is able to move an authenticated object', async () => {
    const objectAdminDeleteSendSpy = vi.spyOn(ObjectAdminDelete, 'send')
    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/move`,
      payload: {
        sourceKey: 'authenticated/move-orig.png',
        destinationKey: 'authenticated/move-new.png',
        bucketId: 'bucket2',
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.copyObject).toHaveBeenCalled()
    expect(objectAdminDeleteSendSpy).toHaveBeenCalled()
  })

  test('can move objects across buckets respecting RLS', async () => {
    const objectAdminDeleteSendSpy = vi.spyOn(ObjectAdminDelete, 'send')
    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/move`,
      payload: {
        bucketId: 'bucket2',
        sourceKey: 'authenticated/move-orig-4.png',
        destinationBucket: 'bucket3',
        destinationKey: 'authenticated/move-new.png',
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.copyObject).toHaveBeenCalled()
    expect(objectAdminDeleteSendSpy).toHaveBeenCalled()
  })

  test('cross-bucket move rollback should cleanup destination bucket object', async () => {
    const runId = randomUUID()
    const sourceKey = `authenticated/move-orig-rollback-${runId}.png`
    const destinationKey = `authenticated/move-new-rollback-${runId}.png`
    const destinationBucket = 'bucket3'
    const objectAdminDeleteSendSpy = vi.spyOn(ObjectAdminDelete, 'send')

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjects(seedTx, {
      bucket_id: 'bucket2',
      name: sourceKey,
      owner: '317eadce-631a-4429-a0bb-f19a7a517b4a',
      version: `rollback-version-${runId}`,
      metadata: { mimetype: 'image/png', size: 1234 },
    })
    await seedTx.commit()
    tnx = undefined

    vi.spyOn(S3Backend.prototype, 'headObject').mockRejectedValueOnce(
      new Error('forced move failure')
    )

    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/move`,
      payload: {
        bucketId: 'bucket2',
        sourceKey,
        destinationBucket,
        destinationKey,
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })

    expect(response.statusCode).toBeGreaterThanOrEqual(400)
    expect(S3Backend.prototype.copyObject).toHaveBeenCalled()
    expect(objectAdminDeleteSendSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        name: destinationKey,
        bucketId: destinationBucket,
      })
    )
  })

  test('cannot move objects across buckets because RLS checks', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/move`,
      payload: {
        bucketId: 'bucket2',
        sourceKey: 'authenticated/move-orig-5.png',
        destinationBucket: 'bucket3',
        destinationKey: 'somekey/move-new.png',
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.copyObject).not.toHaveBeenCalled()
    expect(S3Backend.prototype.deleteObjects).not.toHaveBeenCalled()
  })

  test('check if RLS policies are respected: anon user is not able to move an authenticated object', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/move`,
      payload: {
        sourceKey: 'authenticated/move-orig-2.png',
        destinationKey: 'authenticated/move-new-2.png',
        bucketId: 'bucket2',
      },
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.copyObject).not.toHaveBeenCalled()
    expect(S3Backend.prototype.deleteObject).not.toHaveBeenCalled()
  })

  test('user is not able to move an object without auth header', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/move`,
      payload: {
        sourceKey: 'authenticated/move-orig-3.png',
        destinationKey: 'authenticated/move-orig-new-3.png',
        bucketId: 'bucket2',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.copyObject).not.toHaveBeenCalled()
    expect(S3Backend.prototype.deleteObject).not.toHaveBeenCalled()
  })

  test('user is not able to move an object in a non existent bucket', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/move`,
      payload: {
        sourceKey: 'authenticated/move-orig-3.png',
        destinationKey: 'authenticated/move-orig-new-3.png',
        bucketId: 'notfound',
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.copyObject).not.toHaveBeenCalled()
    expect(S3Backend.prototype.deleteObject).not.toHaveBeenCalled()
  })

  test('user is not able to move an non existent object', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/move`,
      payload: {
        sourceKey: 'authenticated/notfound',
        destinationKey: 'authenticated/move-orig-new-3.png',
        bucketId: 'bucket2',
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.copyObject).not.toHaveBeenCalled()
    expect(S3Backend.prototype.deleteObject).not.toHaveBeenCalled()
  })

  test('user is not able to move to an existing key', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: `/object/move`,
      payload: {
        sourceKey: 'authenticated/move-orig-2.png',
        destinationKey: 'authenticated/move-orig-3.png',
        bucketId: 'bucket2',
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.copyObject).not.toHaveBeenCalled()
    expect(S3Backend.prototype.deleteObject).not.toHaveBeenCalled()
  })
})

describe('testing list objects', () => {
  test('searching the bucket root folder', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/list/bucket2',
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
      payload: {
        prefix: '',
        limit: 10,
        offset: 0,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body) as { name: string }[]
    expect(responseJSON).toHaveLength(9)
    const names = responseJSON.map((ele) => ele.name)
    expect(names).toContain('curlimage.jpg')
    expect(names).toContain('private')
    expect(names).toContain('folder')
    expect(names).toContain('authenticated')
    expect(names).toContain('public')
  })

  test('searching a subfolder', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/list/bucket2',
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
      payload: {
        prefix: 'folder',
        limit: 10,
        offset: 0,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body) as { name: string }[]
    expect(responseJSON).toHaveLength(3)
    const names = responseJSON.map((ele) => ele.name)
    expect(names).toContain('only_uid.jpg')
    expect(names).toContain('subfolder')
    expect(names).toContain('UPPER-folder')
  })

  test('searching a non existent prefix', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/list/bucket2',
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
      payload: {
        prefix: 'notfound',
        limit: 10,
        offset: 0,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(0)
  })

  test('checking if limit works', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/list/bucket2',
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
      payload: {
        prefix: '',
        limit: 2,
        offset: 0,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(2)
  })

  test('listobjects: checking if RLS policies are respected', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/list/bucket2',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        prefix: '',
        limit: 10,
        offset: 0,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(2)
  })

  test('return 400 without Auth Header', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/list/bucket2',
      payload: {
        prefix: '',
        limit: 10,
        offset: 0,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('case insensitive search should work', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/list/bucket2',
      payload: {
        prefix: 'PUBLIC/',
        limit: 10,
        offset: 0,
      },
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(2)
  })

  test('test ascending search sorting', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/list/bucket2',
      payload: {
        prefix: 'public/',
        sortBy: {
          column: 'name',
          order: 'asc',
        },
      },
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(2)
    // Byte order (COLLATE "C"): '.' (46) < '2' (50), so sadcat-upload.png < sadcat-upload23.png
    expect(responseJSON[0].name).toBe('sadcat-upload.png')
    expect(responseJSON[1].name).toBe('sadcat-upload23.png')
  })

  test('test descending search sorting', async () => {
    const response = await appInstance.inject({
      method: 'POST',
      url: '/object/list/bucket2',
      payload: {
        prefix: 'public/',
        sortBy: {
          column: 'name',
          order: 'desc',
        },
      },
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(2)
    // Byte order (COLLATE "C"): sadcat-upload23.png > sadcat-upload.png
    expect(responseJSON[0].name).toBe('sadcat-upload23.png')
    expect(responseJSON[1].name).toBe('sadcat-upload.png')
  })

  test('list-v1 should treat % as a literal character when using non-name sorting', async () => {
    const runId = randomUUID()
    const bucketName = 'bucket2'
    const objectNames = [`percent-${runId}/first.txt`, `percent-${runId}/second.txt`]

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjects(
      seedTx,
      objectNames.map((name, idx) => ({
        bucket_id: bucketName,
        name,
        owner: '317eadce-631a-4429-a0bb-f19a7a517b4a',
        version: `${runId}-${idx}`,
        metadata: {
          eTag: `${runId}-${idx}`,
          size: idx + 1,
          mimetype: 'text/plain',
        },
      }))
    )
    await seedTx.commit()
    tnx = undefined

    try {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/object/list/bucket2',
        payload: {
          prefix: '%',
          limit: 100,
          offset: 0,
          sortBy: {
            column: 'created_at',
            order: 'asc',
          },
        },
        headers: {
          authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(response.statusCode).toBe(200)
      const responseJSON = response.json()
      expect(responseJSON).toHaveLength(0)
    } finally {
      const cleanupTx = await getSuperuserPostgrestClient()
      await withDeleteEnabled(cleanupTx, async (db) => {
        await deleteObjectsByName(db, bucketName, objectNames)
      })
      await cleanupTx.commit()
      tnx = undefined
    }
  })

  test('list-v1 should treat _ as a literal character when using non-name sorting', async () => {
    const runId = randomUUID()
    const bucketName = 'bucket2'
    const literalMatch = `wild_${runId}/hit.txt`
    const wildcardOnlyMatch = `wildX${runId}/miss.txt`

    const seedTx = await getSuperuserPostgrestClient()
    await insertObjects(seedTx, [
      {
        bucket_id: bucketName,
        name: literalMatch,
        owner: '317eadce-631a-4429-a0bb-f19a7a517b4a',
        version: `${runId}-literal`,
        metadata: {
          eTag: `${runId}-literal`,
          size: 1,
          mimetype: 'text/plain',
        },
      },
      {
        bucket_id: bucketName,
        name: wildcardOnlyMatch,
        owner: '317eadce-631a-4429-a0bb-f19a7a517b4a',
        version: `${runId}-wildcard`,
        metadata: {
          eTag: `${runId}-wildcard`,
          size: 2,
          mimetype: 'text/plain',
        },
      },
    ])
    await seedTx.commit()
    tnx = undefined

    try {
      const response = await appInstance.inject({
        method: 'POST',
        url: '/object/list/bucket2',
        payload: {
          prefix: `wild_${runId}/`,
          limit: 100,
          offset: 0,
          sortBy: {
            column: 'created_at',
            order: 'asc',
          },
        },
        headers: {
          authorization: `Bearer ${await serviceKeyAsync}`,
        },
      })

      expect(response.statusCode).toBe(200)
      const responseJSON = response.json<{ name: string }[]>()
      expect(responseJSON.map((obj) => obj.name)).toEqual(['hit.txt'])
    } finally {
      const cleanupTx = await getSuperuserPostgrestClient()
      await withDeleteEnabled(cleanupTx, async (db) => {
        await deleteObjectsByName(db, bucketName, [literalMatch, wildcardOnlyMatch])
      })
      await cleanupTx.commit()
      tnx = undefined
    }
  })
})

describe('x-robots-tag header', () => {
  const X_ROBOTS_TEST_BUCKET = 'X_ROBOTS_TEST_BUCKET'
  beforeAll(async () => {
    appInstance = app()
    await appInstance.inject({
      method: 'POST',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
      payload: {
        name: X_ROBOTS_TEST_BUCKET,
      },
    })
    await appInstance.close()
  })

  afterAll(async () => {
    appInstance = app()
    await appInstance.inject({
      method: 'POST',
      url: `/bucket/${X_ROBOTS_TEST_BUCKET}/empty`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    await appInstance.inject({
      method: 'DELETE',
      url: `/bucket/${X_ROBOTS_TEST_BUCKET}`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    await appInstance.close()
  })

  test('defaults x-robots-tag header to none if not specified', async () => {
    const objPath = `${X_ROBOTS_TEST_BUCKET}/test-file-1.txt`

    const createResponse = await appInstance.inject({
      method: 'POST',
      url: `/object/${objPath}`,
      payload: new File(['test'], 'file.txt'),
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    expect(createResponse.statusCode).toBe(200)

    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/authenticated/${objPath}`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['x-robots-tag']).toBe('none')
  })

  test('uses provided x-robots-tag header if set', async () => {
    const objPath = `${X_ROBOTS_TEST_BUCKET}/test-file-2.txt`

    const createResponse = await appInstance.inject({
      method: 'POST',
      url: `/object/${objPath}`,
      payload: new File(['test'], 'file.txt'),
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
        'x-robots-tag': 'all',
      },
    })
    expect(createResponse.statusCode).toBe(200)

    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/authenticated/${objPath}`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['x-robots-tag']).toBe('all')
  })

  test('updates x-robots-tag header on upsert', async () => {
    const objPath = `${X_ROBOTS_TEST_BUCKET}/test-file-3.txt`

    const createResponse = await appInstance.inject({
      method: 'POST',
      url: `/object/${objPath}`,
      payload: new File(['test'], 'file.txt'),
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
        'x-robots-tag': 'max-snippet: 10, notranslate',
      },
    })
    expect(createResponse.statusCode).toBe(200)

    const response = await appInstance.inject({
      method: 'GET',
      url: `/object/authenticated/${objPath}`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['x-robots-tag']).toBe('max-snippet: 10, notranslate')

    const createResponse2 = await appInstance.inject({
      method: 'POST',
      url: `/object/${objPath}`,
      payload: new File(['test'], 'file.txt'),
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
        'x-upsert': 'true',
        'x-robots-tag': 'nofollow',
      },
    })
    expect(createResponse2.statusCode).toBe(200)

    const response2 = await appInstance.inject({
      method: 'GET',
      url: `/object/authenticated/${objPath}`,
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
      },
    })
    expect(response2.statusCode).toBe(200)
    expect(response2.headers['x-robots-tag']).toBe('nofollow')
  })

  test('rejects invalid x-robots-tag header with proper error', async () => {
    const objPath = `${X_ROBOTS_TEST_BUCKET}/test-file-invalid.txt`

    const createResponse = await appInstance.inject({
      method: 'POST',
      url: `/object/${objPath}`,
      payload: new File(['test'], 'file.txt'),
      headers: {
        authorization: `Bearer ${await serviceKeyAsync}`,
        'x-robots-tag': 'invalidrule',
      },
    })

    expect(createResponse.statusCode).toBe(400)
    expect(createResponse.json()).toMatchObject({
      statusCode: '400',
      error: 'invalid_x_robots_tag',
      message: 'Invalid X-Robots-Tag header: Invalid X-Robots-Tag rule: "invalidrule"',
    })
  })
})
