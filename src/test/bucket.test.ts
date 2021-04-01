'use strict'
import app from '../app'
import * as utils from '../utils/s3'
import { getConfig } from '../utils/config'
import dotenv from 'dotenv'

dotenv.config({ path: '.env.test' })
const { anonKey } = getConfig()

let mockDeleteObjects: any

beforeAll(() => {
  mockDeleteObjects = jest.spyOn(utils, 'deleteObjects')
  mockDeleteObjects.mockImplementation(() =>
    Promise.resolve({
      $metadata: {
        httpStatusCode: 204,
      },
    })
  )
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
    expect(responseJSON.id).toBe(bucketId)
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
      url: '/object/notfouns',
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
    expect(responseJSON.length).toBe(4)
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
    expect(responseJSON.message).toBe('Successfully deflated')
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
