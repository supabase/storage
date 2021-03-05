'use strict'
import app from '../app'
import * as utils from '../utils/s3'
import { getConfig } from '../utils/config'
import dotenv from 'dotenv'
import FormData from 'form-data'
import fs from 'fs'

dotenv.config({ path: '.env.test' })
const { anonKey } = getConfig()

let mockGetObject: any, mockUploadObject: any

beforeAll(() => {
  mockGetObject = jest.spyOn(utils, 'getObject')
  mockGetObject.mockImplementation(() =>
    Promise.resolve({
      $metadata: {
        httpStatusCode: 200,
      },
      CacheControl: undefined,
      ContentDisposition: undefined,
      ContentEncoding: undefined,
      ContentLength: 3746,
      ContentType: 'image/png',
      Metadata: {},
    })
  )

  mockUploadObject = jest.spyOn(utils, 'uploadObject')
  mockUploadObject.mockImplementation(() =>
    Promise.resolve({
      $metadata: {
        httpStatusCode: 200,
      },
      Bucket: 'xxx',
      Key: 'bjhaohmqunupljrqypxz/bucket2/authenticated/sadcat-upload41.png',
    })
  )
})

afterEach(() => {
  jest.clearAllMocks()
})

/*
 * GET /object/:id
 */
test('authenticated user is able to read authenticated resource', async () => {
  const response = await app().inject({
    method: 'GET',
    url: '/object/bucket2/authenticated/casestudy.png',
    headers: {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
    },
  })
  expect(response.statusCode).toBe(200)
  expect(mockGetObject).toBeCalled()
})

test('anon user is not able to read authenticated resource', async () => {
  const response = await app().inject({
    method: 'GET',
    url: '/object/bucket2/authenticated/casestudy.png',
    headers: {
      authorization: `Bearer ${anonKey}`,
    },
  })
  expect(response.statusCode).toBe(406)
  expect(mockGetObject).not.toHaveBeenCalled()
})

test('user is not able to read a resource without Auth header', async () => {
  const response = await app().inject({
    method: 'GET',
    url: '/object/bucket2/authenticated/casestudy.png',
  })
  expect(response.statusCode).toBe(403)
  expect(mockGetObject).not.toHaveBeenCalled()
})

test('return 403 when reading a non existent object', async () => {
  const response = await app().inject({
    method: 'GET',
    url: '/object/bucket2/authenticated/notfound',
  })
  expect(response.statusCode).toBe(403)
  expect(mockGetObject).not.toHaveBeenCalled()
})

test('return 404 when reading a non existent bucket', async () => {
  const response = await app().inject({
    method: 'GET',
    url: '/object/notfound/authenticated/casestudy.png',
  })
  expect(response.statusCode).toBe(403)
  expect(mockGetObject).not.toHaveBeenCalled()
})

/*
 * POST /object/:id
 */
test('authenticated user is able to upload authenticated resource', async () => {
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
  expect(mockUploadObject).toBeCalled()
  expect(response.body).toBe(`{"Key":"bjhaohmqunupljrqypxz/bucket2/authenticated/casestudy1.png"}`)
})

test('anon user is not able to upload authenticated resource', async () => {
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
  expect(response.statusCode).toBe(403)
  expect(mockUploadObject).not.toHaveBeenCalled()
  expect(response.body).toBe(`new row violates row-level security policy for table "objects"`)
})

test('user is not able to upload a resource without Auth header', async () => {
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
  expect(response.statusCode).toBe(403)
  expect(mockGetObject).not.toHaveBeenCalled()
})

test('return 404 when uploading to a non existent bucket', async () => {
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
  expect(response.statusCode).toBe(406)
  expect(mockGetObject).not.toHaveBeenCalled()
})
