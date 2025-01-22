'use strict'

import FormData from 'form-data'
import fs from 'fs'
import app from '../app'
import { getConfig, mergeConfig } from '../config'
import { signJWT } from '@internal/auth'
import { Obj, backends } from '../storage'
import { useMockObject, useMockQueue } from './common'
import { getServiceKeyUser, getPostgresConnection } from '@internal/database'
import { Knex } from 'knex'
import { ErrorCode, StorageBackendError } from '@internal/errors'

const { jwtSecret, serviceKey, tenantId } = getConfig()
const anonKey = process.env.ANON_KEY || ''
const S3Backend = backends.S3Backend

let tnx: Knex.Transaction | undefined
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

useMockObject()
useMockQueue()

beforeEach(() => {
  getConfig({ reload: true })
})

afterEach(async () => {
  if (tnx) {
    await tnx.commit()
  }
})

/*
 * GET /object/:id
 */
describe('testing GET object', () => {
  test('check if RLS policies are respected: authenticated user is able to read authenticated resource', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['last-modified']).toBe('Thu, 12 Aug 2021 16:00:00 GMT')
    expect(S3Backend.prototype.getObject).toBeCalled()
  })

  test('check if RLS policies are respected: authenticated user is able to read authenticated resource without /authenticated prefix', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['last-modified']).toBe('Thu, 12 Aug 2021 16:00:00 GMT')
    expect(S3Backend.prototype.getObject).toBeCalled()
  })

  test('forward 304 and If-Modified-Since/If-None-Match headers', async () => {
    const mockGetObject = jest.spyOn(S3Backend.prototype, 'getObject')
    mockGetObject.mockRejectedValue({
      $metadata: {
        httpStatusCode: 304,
      },
    })
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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

  test('cannot get authenticated object info without the /authenticated prefix if no jwt is provided', async () => {
    const response = await app().inject({
      method: 'HEAD',
      url: '/object/bucket2/authenticated/casestudy.png',
    })
    expect(response.statusCode).toBe(400)
  })

  test('get public object info without using the /public prefix', async () => {
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png?download',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(S3Backend.prototype.getObject).toBeCalled()
    expect(response.headers).toEqual(
      expect.objectContaining({
        'content-disposition': `attachment;`,
      })
    )
  })

  test('force downloading file with a custom name', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png?download=testname.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(S3Backend.prototype.getObject).toBeCalled()
    expect(response.headers).toEqual(
      expect.objectContaining({
        'content-disposition': `attachment; filename=testname.png; filename*=UTF-8''testname.png;`,
      })
    )
  })

  test('check if RLS policies are respected: anon user is not able to read authenticated resource', async () => {
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png',
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.getObject).not.toHaveBeenCalled()
  })

  test('user is not able to read a resource without Auth header without the /authenticated prefix', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/bucket2/authenticated/casestudy.png',
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.getObject).not.toHaveBeenCalled()
  })

  test('return 400 when reading a non existent object', async () => {
    const response = await app().inject({
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
    const response = await app().inject({
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

    const response = await app().inject({
      method: 'POST',
      url: '/object/bucket2/authenticated/casestudy1.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toBeCalled()
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
    })

    const response = await app().inject({
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
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
    })

    const response = await app().inject({
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
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
      'content-type': 'image/jpeg',
    })

    const response = await app().inject({
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
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
      ...form.getHeaders(),
    })

    const response = await app().inject({
      method: 'POST',
      url: '/object/bucket2/sadcat-upload3012.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()

    const client = await getSuperuserPostgrestClient()

    const object = await client
      .table('objects')
      .select('*')
      .where('name', 'sadcat-upload3012.png')
      .where('bucket_id', 'bucket2')
      .first()

    expect(object).not.toBeFalsy()
    expect(object?.user_metadata).toEqual({
      test1: 'test1',
      test2: 'test2',
    })
  })

  test('successfully uploading an object with custom metadata using stream', async () => {
    const file = fs.createReadStream(`./src/test/assets/sadcat.jpg`)

    const headers = {
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
      'x-metadata': Buffer.from(
        JSON.stringify({
          test1: 'test1',
          test2: 'test2',
        })
      ).toString('base64'),
    }

    const response = await app().inject({
      method: 'POST',
      url: '/object/bucket2/sadcat-upload3018.png',
      headers,
      payload: file,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()

    const client = await getSuperuserPostgrestClient()

    const object = await client
      .table('objects')
      .select('*')
      .where('name', 'sadcat-upload3018.png')
      .where('bucket_id', 'bucket2')
      .first()

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
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
    })

    const uploadResponse = await app().inject({
      method: 'POST',
      url: '/object/bucket2/sadcat-upload3019.png',
      headers: {
        ...headers,
        ...form.getHeaders(),
      },
      payload: form,
    })
    expect(uploadResponse.statusCode).toBe(200)

    const response = await app().inject({
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

  test('return 422 when uploading an object with a not allowed mime-type', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
      'content-type': 'image/png',
    })

    const response = await app().inject({
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

  test('can create an empty folder when mime-type is set', async () => {
    const form = new FormData()
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
    })

    form.append('file', Buffer.alloc(0))

    const response = await app().inject({
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
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
    })

    form.append('file', Buffer.alloc(1))

    const response = await app().inject({
      method: 'POST',
      url: '/object/public-limit-mime-types/nested-2/.emptyFolderPlaceholder',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
  })

  test('return 422 when uploading an object with a malformed mime-type', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${serviceKey}`,
      'x-upsert': 'true',
      'content-type': 'thisisnotarealmimetype',
    })

    const response = await app().inject({
      method: 'POST',
      url: '/object/public-limit-mime-types/sadcat-upload23.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(await response.json()).toEqual({
      error: 'invalid_mime_type',
      message: `mime type thisisnotarealmimetype is not supported`,
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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
    jest.spyOn(S3Backend.prototype, 'uploadObject').mockRejectedValue(
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

    const createObjectResponse = await app().inject({
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
    const objectResponse = await db
      .from<Obj>('objects')
      .select('*')
      .where({
        name: OBJECT_NAME,
        bucket_id: BUCKET_ID,
      })
      .first()

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

    const response = await app().inject({
      method: 'POST',
      url: '/object/bucket2/authenticated/binary-casestudy1.png',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toBeCalled()
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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

  test('return 400 when uploading to object with no file name', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${anonKey}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
      'x-upsert': 'true',
    }

    const response = await app().inject({
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
    jest.spyOn(S3Backend.prototype, 'uploadObject').mockRejectedValue(
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

    const createObjectResponse = await app().inject({
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
    const objectResponse = await db
      .from<Obj>('objects')
      .select('*')
      .where({
        name: OBJECT_NAME,
        bucket_id: BUCKET_ID,
      })
      .first()
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

    const response = await app().inject({
      method: 'PUT',
      url: '/object/bucket2/authenticated/cat.jpg',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toBeCalled()
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
      method: 'PUT',
      url: '/object/bucket2/authenticated/cat.jpg',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toBeCalled()
    expect(await response.json()).toEqual(
      expect.objectContaining({
        Id: expect.any(String),
        Key: 'bucket2/authenticated/cat.jpg',
      })
    )
  })

  test('check if RLS policies are respected: anon user is not able to update authenticated resource', async () => {
    const path = './src/test/assets/sadcat.jpg'
    const { size } = fs.statSync(path)

    const headers = {
      authorization: `Bearer ${anonKey}`,
      'Content-Length': size,
      'Content-Type': 'image/jpeg',
    }

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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

    const response = await app().inject({
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
    const response = await app().inject({
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
    expect(S3Backend.prototype.copyObject).toBeCalled()
    const jsonResponse = await response.json()
    expect(jsonResponse.Key).toBe(`bucket2/authenticated/casestudy11.png`)
  })

  test('can copy objects across buckets', async () => {
    const response = await app().inject({
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
    expect(S3Backend.prototype.copyObject).toBeCalled()
    const jsonResponse = await response.json()

    expect(jsonResponse.Key).toBe(`bucket3/authenticated/casestudy11.png`)
  })

  test('can copy objects keeping their metadata', async () => {
    const copiedKey = 'casestudy-2349.png'
    const response = await app().inject({
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
    expect(S3Backend.prototype.copyObject).toBeCalled()
    const jsonResponse = response.json()
    expect(jsonResponse.Key).toBe(`bucket2/authenticated/${copiedKey}`)

    const conn = await getSuperuserPostgrestClient()
    const object = await conn
      .table('objects')
      .select('*')
      .where('bucket_id', 'bucket2')
      .where('name', `authenticated/${copiedKey}`)
      .first()

    expect(object).not.toBeFalsy()
    expect(object.user_metadata).toEqual({
      test1: 1234,
    })
  })

  test('can copy objects to itself overwriting their metadata', async () => {
    const copiedKey = 'casestudy-2349.png'
    const response = await app().inject({
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
    expect(S3Backend.prototype.copyObject).toBeCalled()
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
    const object = await conn
      .table('objects')
      .select('*')
      .where('bucket_id', 'bucket2')
      .where('name', `authenticated/${copiedKey}`)
      .first()

    expect(object).not.toBeFalsy()
    expect(object.user_metadata).toEqual({
      newMetadata: 'test1',
    })
    expect(object.metadata).toEqual(
      expect.objectContaining({
        cacheControl: 'max-age=999',
        mimetype: 'image/gif',
      })
    )
  })

  test('can copy objects excluding their metadata', async () => {
    const copiedKey = 'casestudy-2450.png'
    const response = await app().inject({
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
    expect(S3Backend.prototype.copyObject).toBeCalled()
    const jsonResponse = response.json()
    expect(jsonResponse.Key).toBe(`bucket2/authenticated/${copiedKey}`)

    const conn = await getSuperuserPostgrestClient()
    const object = await conn
      .table('objects')
      .select('*')
      .where('bucket_id', 'bucket2')
      .where('name', `authenticated/${copiedKey}`)
      .first()

    expect(object).not.toBeFalsy()
    expect(object.user_metadata).toBeNull()
  })

  test('cannot copy objects across buckets when RLS dont allow it', async () => {
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
      method: 'DELETE',
      url: '/object/bucket2/authenticated/delete.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.deleteObject).toBeCalled()
  })

  test('check if RLS policies are respected: anon user is not able to delete authenticated resource', async () => {
    const response = await app().inject({
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
    const response = await app().inject({
      method: 'DELETE',
      url: '/object/bucket2/authenticated/delete1.png',
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.deleteObject).not.toHaveBeenCalled()
  })

  test('return 400 when delete from a non existent bucket', async () => {
    const response = await app().inject({
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
    const response = await app().inject({
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
  test('check if RLS policies are respected: authenticated user is able to delete authenticated resource', async () => {
    const response = await app().inject({
      method: 'DELETE',
      url: '/object/bucket2',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        prefixes: [...Array(10001).keys()].map((i) => `authenticated/${i}`),
      },
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.deleteObjects).toBeCalled()

    const result = JSON.parse(response.body)
    expect(result).toHaveLength(10001)
    expect(result[0].name).toBe('authenticated/0')
    expect(result[1].name).toBe('authenticated/1')
  })

  test('check if RLS policies are respected: anon user is not able to delete authenticated resource', async () => {
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    expect(S3Backend.prototype.deleteObjects).toBeCalled()
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
    const response = await app().inject({
      method: 'POST',
      url: '/object/sign/bucket2/authenticated/cat.jpg',
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
  })

  test('check if RLS policies are respected: anon user is not able to generate signedURL for authenticated resource', async () => {
    const response = await app().inject({
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
    const response = await app().inject({
      method: 'POST',
      url: '/object/sign/bucket2/authenticated/cat.jpg',
      payload: {
        expiresIn: 1000,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('return 400 when generate signed urls from a non existent bucket', async () => {
    const response = await app().inject({
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
    const response = await app().inject({
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
})

/**
 * POST /upload/sign/:bucketName/*
 */
describe('testing generating signed URL for upload', () => {
  test('check if RLS policies are respected: authenticated user is able to sign upload URL for a resource', async () => {
    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'authenticated/cat1.jpg'

    const response = await app().inject({
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
    const objectResponse = await db
      .from<Obj>('objects')
      .select('*')
      .where({
        name: OBJECT_NAME,
        bucket_id: BUCKET_ID,
      })
      .first()
    expect(objectResponse).toBe(undefined)
  })

  test('check if RLS policies are respected: anon user is not able to sign upload URL for authenticated resource', async () => {
    const BUCKET_ID = 'bucket2'
    const OBJECT_NAME = 'authenticated/cat1.jpg'

    const response = await app().inject({
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
    const objectResponse = await db
      .from<Obj>('objects')
      .select('*')
      .where({
        name: OBJECT_NAME,
        bucket_id: BUCKET_ID,
      })
      .first()
    expect(objectResponse).toBe(undefined)
  })

  test('user is not able to sign a upload url without Auth header', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/upload/sign/bucket2/authenticated/cat.jpg',
    })
    expect(response.statusCode).toBe(400)
  })

  test('return 400 when generating signed upload urls from a non existent bucket', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/upload/sign/notfound/authenticated/cat.jpg',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('signing upload url of a non existent key', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/upload/sign/bucket2/authenticated/notfound.jpg',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
  })

  test('signing upload url of an existent key', async () => {
    const response = await app().inject({
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

    const jwtToken = await signJWT({ owner, url: urlToSign }, jwtSecret, 100)
    const response = await app().inject({
      method: 'PUT',
      url: `/object/upload/sign/${urlToSign}?token=${jwtToken}`,
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()

    // check that row has neccessary data
    const db = await getSuperuserPostgrestClient()
    const objectResponse = await db
      .from<Obj>('objects')
      .select('*')
      .where({
        name: OBJECT_NAME,
        bucket_id: BUCKET_ID,
      })
      .first()
    expect(objectResponse?.owner).toBe(owner)

    // remove row to not to break other tests
    await db
      .from<Obj>('objects')
      .where({
        name: OBJECT_NAME,
        bucket_id: BUCKET_ID,
      })
      .delete()
  })

  test('upload object without a token', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      'content-type': 'image/jpeg',
    })

    const response = await app().inject({
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

    const response = await app().inject({
      method: 'PUT',
      url: `/object/upload/sign/bucket2/public/sadcat-upload1.png?token=xxx`,
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
    expect(S3Backend.prototype.uploadObject).not.toHaveBeenCalled()
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

    const jwtToken = await signJWT({ owner, url: urlToSign }, jwtSecret, -1)
    const response = await app().inject({
      method: 'PUT',
      url: `/object/upload/sign/${urlToSign}?token=${jwtToken}`,
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(400)
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
    const resp = await app().inject({
      method: 'POST',
      url: `/object/${urlToSign}`,
      payload: createUpload(),
      headers: {
        'x-upsert': 'true',
        authorization: serviceKey,
      },
    })

    expect(resp.statusCode).toBe(200)

    // generate signed upload url with upsert
    const signedUrlResp = await app().inject({
      method: 'POST',
      url: `/object/upload/sign/${urlToSign}`,
      headers: {
        'x-upsert': 'true',
        authorization: serviceKey,
      },
    })
    expect(signedUrlResp.statusCode).toBe(200)

    const jwtToken = (await signedUrlResp.json()).token
    const response = await app().inject({
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
    const resp = await app().inject({
      method: 'POST',
      url: `/object/${urlToSign}`,
      payload: createUpload(),
      headers: {
        authorization: serviceKey,
      },
    })

    expect(resp.statusCode).toBe(200)

    const jwtToken = await signJWT({ owner, url: urlToSign }, jwtSecret, 100)
    const response = await app().inject({
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
    const response = await app().inject({
      method: 'POST',
      url: '/object/sign/bucket2',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1000,
        paths: [...Array(10001).keys()].map((i) => `authenticated/${i}`),
      },
    })
    expect(response.statusCode).toBe(200)
    const result = JSON.parse(response.body)
    expect(result).toHaveLength(10001)
  })

  test('check if RLS policies are respected: anon user is not able to generate signedURLs for authenticated resource', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/sign/bucket2',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        expiresIn: 1000,
        paths: [...Array(10001).keys()].map((i) => `authenticated/${i}`),
      },
    })
    expect(response.statusCode).toBe(200)
    const result = JSON.parse(response.body)
    expect(result[0].error).toBe('Either the object does not exist or you do not have access to it')
  })

  test('user is not able to generate signedURLs without Auth header', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/sign/bucket2',
      payload: {
        expiresIn: 1000,
        paths: [...Array(10001).keys()].map((i) => `authenticated/${i}`),
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('return 400 when generate signed urls from a non existent bucket', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/sign/notfound',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        expiresIn: 1000,
        paths: [...Array(10001).keys()].map((i) => `authenticated/${i}`),
      },
    })
    expect(response.statusCode).toBe(200)
    const result = JSON.parse(response.body)
    expect(result[0].error).toBe('Either the object does not exist or you do not have access to it')
  })

  test('signing url of a non existent key', async () => {
    const response = await app().inject({
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
})

/**
 * GET /public/
 */
// these tests are written in bucket.test.ts since its easier

/**
 * GET /sign/
 */
describe('testing retrieving signed URL', () => {
  test('get object with a token', async () => {
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const jwtToken = await signJWT({ url: urlToSign }, jwtSecret, 100)
    const response = await app().inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${jwtToken}`,
    })
    expect(response.statusCode).toBe(200)
    expect(response.headers['etag']).toBe('abc')
    expect(response.headers['last-modified']).toBe('Thu, 12 Aug 2021 16:00:00 GMT')
  })

  test('forward 304 and If-Modified-Since/If-None-Match headers', async () => {
    const mockGetObject = jest.spyOn(S3Backend.prototype, 'getObject')
    mockGetObject.mockRejectedValue({
      $metadata: {
        httpStatusCode: 304,
      },
    })
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const jwtToken = await signJWT({ url: urlToSign }, jwtSecret, 100)
    const response = await app().inject({
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

  test('get object without a token', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/sign/bucket2/public/sadcat-upload.png',
    })
    expect(response.statusCode).toBe(400)
  })

  test('get object with a malformed JWT', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/sign/bucket2/public/sadcat-upload.png?token=xxx',
    })
    expect(response.statusCode).toBe(400)
  })

  test('get object with an expired JWT', async () => {
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const expiredJWT = await signJWT({ url: urlToSign }, jwtSecret, -1)
    const response = await app().inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${expiredJWT}`,
    })
    expect(response.statusCode).toBe(400)
  })
})

describe('testing move object', () => {
  test('check if RLS policies are respected: authenticated user is able to move an authenticated object', async () => {
    const response = await app().inject({
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
    expect(S3Backend.prototype.deleteObjects).toHaveBeenCalled()
  })

  test('can move objects across buckets respecting RLS', async () => {
    const response = await app().inject({
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
    expect(S3Backend.prototype.deleteObjects).toHaveBeenCalled()
  })

  test('cannot move objects across buckets because RLS checks', async () => {
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
      method: 'POST',
      url: '/object/list/bucket2',
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
      payload: {
        prefix: '',
        limit: 10,
        offset: 0,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(9)
    const names = responseJSON.map((ele: any) => ele.name)
    expect(names).toContain('curlimage.jpg')
    expect(names).toContain('private')
    expect(names).toContain('folder')
    expect(names).toContain('authenticated')
    expect(names).toContain('public')
  })

  test('searching a subfolder', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/list/bucket2',
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
      payload: {
        prefix: 'folder',
        limit: 10,
        offset: 0,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(2)
    const names = responseJSON.map((ele: any) => ele.name)
    expect(names).toContain('only_uid.jpg')
    expect(names).toContain('subfolder')
  })

  test('searching a non existent prefix', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/list/bucket2',
      headers: {
        authorization: `Bearer ${serviceKey}`,
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
    const response = await app().inject({
      method: 'POST',
      url: '/object/list/bucket2',
      headers: {
        authorization: `Bearer ${serviceKey}`,
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
    const response = await app().inject({
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
    const response = await app().inject({
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
    const response = await app().inject({
      method: 'POST',
      url: '/object/list/bucket2',
      payload: {
        prefix: 'PUBLIC/',
        limit: 10,
        offset: 0,
      },
      headers: {
        authorization: `Bearer ${serviceKey}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(2)
  })

  test('test ascending search sorting', async () => {
    const response = await app().inject({
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
        authorization: `Bearer ${serviceKey}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(2)
    expect(responseJSON[0].name).toBe('sadcat-upload23.png')
    expect(responseJSON[1].name).toBe('sadcat-upload.png')
  })

  test('test descending search sorting', async () => {
    const response = await app().inject({
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
        authorization: `Bearer ${serviceKey}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toHaveLength(2)
    expect(responseJSON[0].name).toBe('sadcat-upload.png')
    expect(responseJSON[1].name).toBe('sadcat-upload23.png')
  })
})
