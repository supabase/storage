'use strict'
import dotenv from 'dotenv'
import app from '../app'
import { S3Backend } from '../storage/backend'

dotenv.config({ path: '.env.test' })
const anonKey = process.env.ANON_KEY || ''

beforeAll(() => {
  jest.spyOn(S3Backend.prototype, 'deleteObjects').mockImplementation(() => {
    return Promise.resolve()
  })

  jest.spyOn(S3Backend.prototype, 'getObject').mockImplementation(() => {
    return Promise.resolve({
      metadata: {
        httpStatusCode: 200,
        size: 3746,
        mimetype: 'image/png',
        lastModified: new Date('Thu, 12 Aug 2021 16:00:00 GMT'),
        eTag: 'abc',
        cacheControl: 'no-cache',
        contentLength: 3746,
      },
      httpStatusCode: 200,
      body: Buffer.from(''),
    })
  })
})

beforeEach(() => {
  jest.clearAllMocks()
})

/*
 * GET /bucket/:id
 */
// @todo add RLS tests for buckets
describe('testing GET bucket', () => {
  test('user is able to get bucket details', async () => {
    const bucketId = 'bucket2'
    const response = await app().inject({
      method: 'GET',
      url: `/bucket/${bucketId}`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toMatchObject({
      id: bucketId,
      name: bucketId,
      public: false,
      file_size_limit: null,
      allowed_mime_types: null,
    })
  })

  test('checking RLS: anon user is not able to get bucket details', async () => {
    const bucketId = 'bucket2'
    const response = await app().inject({
      method: 'GET',
      url: `/bucket/${bucketId}`,
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to get bucket details without Auth header', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/bucket/bucket2',
    })
    expect(response.statusCode).toBe(400)
  })

  test('return 404 when reading a non existent bucket', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/notfound',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(404)
  })
})

/*
 * GET /bucket
 */
describe('testing GET all buckets', () => {
  test('user is able to get all buckets', async () => {
    const response = await app().inject({
      method: 'GET',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON.length).toBeGreaterThanOrEqual(10)
    expect(responseJSON[0]).toMatchObject({
      id: expect.any(String),
      name: expect.any(String),
      public: expect.any(Boolean),
      file_size_limit: null,
      allowed_mime_types: null,
    })
  })

  test('checking RLS: anon user is not able to get all buckets', async () => {
    const response = await app().inject({
      method: 'GET',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON.length).toBe(0)
  })

  test('user is not able to all buckets details without Auth header', async () => {
    const response = await app().inject({
      method: 'GET',
      url: `/bucket`,
    })
    expect(response.statusCode).toBe(400)
  })
})
/*
 * POST /bucket
 */
describe('testing POST bucket', () => {
  test('user is able to create a bucket', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        name: 'newbucket',
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON.name).toBe('newbucket')
  })

  test('user is not able to create a bucket with a /', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        name: 'newbucket/test',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(response.json()).toEqual({
      error: 'Invalid Input',
      message: 'Bucket name invalid',
      statusCode: '400',
    })
  })

  test('checking RLS: anon user is not able to create a bucket', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        name: 'newbucket1',
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to create a bucket without Auth header', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/bucket`,
      payload: {
        name: 'newbucket1',
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to create a bucket with the same name', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        name: 'bucket2',
      },
    })
    expect(response.statusCode).toBe(400)
  })
})

/*
 * PUT /bucket
 */
describe('testing public bucket functionality', () => {
  test('user is able to make a bucket public and private', async () => {
    const bucketId = 'public-bucket'
    const makePublicResponse = await app().inject({
      method: 'PUT',
      url: `/bucket/${bucketId}`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        public: true,
      },
    })
    expect(makePublicResponse.statusCode).toBe(200)
    const makePublicJSON = JSON.parse(makePublicResponse.body)
    expect(makePublicJSON.message).toBe('Successfully updated')

    const publicResponse = await app().inject({
      method: 'GET',
      url: `/object/public/public-bucket/favicon.ico`,
    })
    expect(publicResponse.statusCode).toBe(200)
    expect(publicResponse.headers['etag']).toBe('abc')
    expect(publicResponse.headers['last-modified']).toBe('Thu, 12 Aug 2021 16:00:00 GMT')

    const mockGetObject = jest.spyOn(S3Backend.prototype, 'getObject')
    mockGetObject.mockRejectedValue({
      $metadata: {
        httpStatusCode: 304,
      },
    })
    const notModifiedResponse = await app().inject({
      method: 'GET',
      url: `/object/public/public-bucket/favicon.ico`,
      headers: {
        'if-modified-since': 'Thu, 12 Aug 2021 16:00:00 GMT',
        'if-none-match': 'abc',
      },
    })
    expect(notModifiedResponse.statusCode).toBe(304)
    expect(mockGetObject.mock.calls[1][3]).toMatchObject({
      ifModifiedSince: 'Thu, 12 Aug 2021 16:00:00 GMT',
      ifNoneMatch: 'abc',
    })

    const makePrivateResponse = await app().inject({
      method: 'PUT',
      url: `/bucket/${bucketId}`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
      payload: {
        public: false,
      },
    })
    expect(makePrivateResponse.statusCode).toBe(200)
    const makePrivateJSON = JSON.parse(makePrivateResponse.body)
    expect(makePrivateJSON.message).toBe('Successfully updated')

    const privateResponse = await app().inject({
      method: 'GET',
      url: `/object/public/public-bucket/favicon.ico`,
    })
    expect(privateResponse.statusCode).toBe(400)
  })

  test('checking RLS: anon user is not able to update a bucket', async () => {
    const bucketId = 'public-bucket'
    const response = await app().inject({
      method: 'PUT',
      url: `/bucket/${bucketId}`,
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        public: true,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to update a bucket without a auth header', async () => {
    const bucketId = 'public-bucket'
    const response = await app().inject({
      method: 'PUT',
      url: `/bucket/${bucketId}`,
      payload: {
        public: true,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to update a non-existent bucket', async () => {
    const bucketId = 'notfound'
    const response = await app().inject({
      method: 'PUT',
      url: `/bucket/${bucketId}`,
      payload: {
        public: true,
      },
    })
    expect(response.statusCode).toBe(400)
  })
})

describe('testing DELETE bucket', () => {
  test('user is able to delete a bucket', async () => {
    const bucketId = 'bucket4'
    const response = await app().inject({
      method: 'DELETE',
      url: `/bucket/${bucketId}`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON.message).toBe('Successfully deleted')
  })

  test('checking RLS: anon user is not able to delete a bucket', async () => {
    const bucketId = 'bucket5'
    const response = await app().inject({
      method: 'DELETE',
      url: `/bucket/${bucketId}`,
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to delete bucket without Auth header', async () => {
    const bucketId = 'bucket5'
    const response = await app().inject({
      method: 'DELETE',
      url: `/bucket/${bucketId}`,
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to delete bucket a non empty bucket', async () => {
    const bucketId = 'bucket2'
    const response = await app().inject({
      method: 'DELETE',
      url: `/bucket/${bucketId}`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to delete a non-existent bucket', async () => {
    const bucketId = 'notfound'
    const response = await app().inject({
      method: 'DELETE',
      url: `/bucket/${bucketId}`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
  })
})

describe('testing EMPTY bucket', () => {
  test('user is able to empty a bucket', async () => {
    const bucketId = 'bucket3'
    const response = await app().inject({
      method: 'POST',
      url: `/bucket/${bucketId}/empty`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON.message).toBe('Successfully emptied')
  })

  test('user is able to empty a bucket with a service key', async () => {
    const bucketId = 'bucket3'
    const response = await app().inject({
      method: 'POST',
      url: `/bucket/${bucketId}/empty`,
      headers: {
        authorization: `Bearer ${process.env.SERVICE_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON.message).toBe('Successfully emptied')
  })

  test('user is able to delete a bucket', async () => {
    const bucketId = 'bucket3'
    const response = await app().inject({
      method: 'POST',
      url: `/bucket/${bucketId}/empty`,
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to empty a bucket without Auth Header', async () => {
    const bucketId = 'bucket3'
    const response = await app().inject({
      method: 'POST',
      url: `/bucket/${bucketId}/empty`,
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is not able to empty a non existent bucket', async () => {
    const bucketId = 'notfound'
    const response = await app().inject({
      method: 'POST',
      url: `/bucket/${bucketId}/empty`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('user is able to empty an already empty bucket', async () => {
    const bucketId = 'bucket5'
    const response = await app().inject({
      method: 'POST',
      url: `/bucket/${bucketId}/empty`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
  })
})
