'use strict'
import dotenv from 'dotenv'
import FormData from 'form-data'
import fs from 'fs'
import app from '../app'
import { getConfig } from '../utils/config'
import { signJWT } from '../utils/index'
import { S3Backend } from '../backend/s3'
import { PostgrestClient } from '@supabase/postgrest-js'
import { Obj } from '../types/types'
import { convertErrorToStorageBackendError } from '../utils/errors'

dotenv.config({ path: '.env.test' })
const ENV = process.env
const { anonKey, jwtSecret, serviceKey, postgrestURL } = getConfig()

function getSuperuserPostgrestClient() {
  return new PostgrestClient(postgrestURL, {
    headers: {
      apiKey: anonKey,
      Authorization: `Bearer ${serviceKey}`,
    },
    schema: 'storage',
  })
}

beforeEach(() => {
  process.env = { ...ENV }

  jest.spyOn(S3Backend.prototype, 'getObject').mockResolvedValue({
    metadata: {
      httpStatusCode: 200,
      size: 3746,
      mimetype: 'image/png',
      lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
      eTag: 'abc',
    },
    body: Buffer.from(''),
  })

  jest.spyOn(S3Backend.prototype, 'uploadObject').mockResolvedValue({
    httpStatusCode: 200,
    size: 3746,
    mimetype: 'image/png',
  })

  jest.spyOn(S3Backend.prototype, 'copyObject').mockResolvedValue({
    httpStatusCode: 200,
    size: 3746,
    mimetype: 'image/png',
  })

  jest.spyOn(S3Backend.prototype, 'deleteObject').mockResolvedValue({})

  jest.spyOn(S3Backend.prototype, 'deleteObjects').mockResolvedValue({})

  jest.spyOn(S3Backend.prototype, 'headObject').mockResolvedValue({
    httpStatusCode: 200,
    size: 3746,
    mimetype: 'image/png',
  })
})

beforeEach(() => {
  jest.clearAllMocks()
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
    expect(mockGetObject.mock.calls[0][2]).toMatchObject({
      ifModifiedSince: 'Thu, 12 Aug 2021 16:00:00 GMT',
      ifNoneMatch: 'abc',
    })
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

  test('user is not able to read a resource without Auth header', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/authenticated/bucket2/authenticated/casestudy.png',
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
    })

    const response = await app().inject({
      method: 'POST',
      url: '/object/bucket2/authenticated/casestudy1.png',
      headers,
      payload: form,
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toBeCalled()
    expect(response.body).toBe(`{"Key":"bucket2/authenticated/casestudy1.png"}`)
  })

  test('check if RLS policies are respected: anon user is not able to upload authenticated resource', async () => {
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
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
        statusCode: '42501',
        error: '',
        message: 'new row violates row-level security policy for table "objects"',
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
    expect(S3Backend.prototype.uploadObject).toHaveBeenCalled()
  })

  test('return 400 when exceeding file size limit', async () => {
    process.env.FILE_SIZE_LIMIT = '1'
    const form = new FormData()
    form.append('file', fs.createReadStream(`./src/test/assets/sadcat.jpg`))
    const headers = Object.assign({}, form.getHeaders(), {
      authorization: `Bearer ${anonKey}`,
    })

    const response = await app().inject({
      method: 'POST',
      url: '/object/bucket2/public/sadcat.jpg',
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
      convertErrorToStorageBackendError({
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
      statusCode: '500',
      error: 'Unknown error',
      message: 'S3ServiceException',
    })

    // Ensure that row does not exist in database.
    const postgrest = getSuperuserPostgrestClient()
    const objectResponse = await postgrest
      .from<Obj>('objects')
      .select()
      .match({
        name: OBJECT_NAME,
        bucket_id: BUCKET_ID,
      })
      .maybeSingle()
    expect(objectResponse.error).toBe(null)
    expect(objectResponse.data).toBe(null)
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
    }

    const response = await app().inject({
      method: 'POST',
      url: '/object/bucket2/authenticated/binary-casestudy1.png',
      headers,
      payload: fs.createReadStream(path),
    })
    expect(response.statusCode).toBe(200)
    expect(S3Backend.prototype.uploadObject).toBeCalled()
    expect(response.body).toBe(`{"Key":"bucket2/authenticated/binary-casestudy1.png"}`)
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
        statusCode: '42501',
        error: '',
        message: 'new row violates row-level security policy for table "objects"',
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
    process.env.FILE_SIZE_LIMIT = '1'
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
      convertErrorToStorageBackendError({
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
      error: 'Unknown error',
      message: 'S3ServiceException',
    })

    // Ensure that row does not exist in database.
    const postgrest = getSuperuserPostgrestClient()
    const objectResponse = await postgrest
      .from<Obj>('objects')
      .select()
      .match({
        name: OBJECT_NAME,
        bucket_id: BUCKET_ID,
      })
      .maybeSingle()
    expect(objectResponse.error).toBe(null)
    expect(objectResponse.data).toBe(null)
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
    expect(response.body).toBe(`{"Key":"bucket2/authenticated/cat.jpg"}`)
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
    // expect(response.body).toBe(`new row violates row-level security policy for table "objects"`)
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
    expect(response.body).toBe(`{"Key":"bucket2/authenticated/cat.jpg"}`)
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
    expect(response.body).toBe(`{"Key":"bucket2/authenticated/casestudy11.png"}`)
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
    expect(mockGetObject.mock.calls[0][2]).toMatchObject({
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
    expect(S3Backend.prototype.deleteObject).toHaveBeenCalled()
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
    expect(responseJSON).toHaveLength(5)
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
