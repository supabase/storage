'use strict'
import app from '../app'
import * as utils from '../utils/s3'
import { getConfig } from '../utils/config'
import { signJWT } from '../utils/index'
import dotenv from 'dotenv'
import FormData from 'form-data'
import fs from 'fs'

dotenv.config({ path: '.env.test' })
const { anonKey, serviceKey } = getConfig()

let mockGetObject: any,
  mockUploadObject: any,
  mockCopyObject: any,
  mockDeleteObject: any,
  mockDeleteObjects: any,
  mockHeadObject: any

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

  mockCopyObject = jest.spyOn(utils, 'copyObject')
  mockCopyObject.mockImplementation(() =>
    Promise.resolve({
      $metadata: {
        httpStatusCode: 200,
      },
      Bucket: 'xxx',
      Key: 'authenticated/casestudy11.png',
    })
  )

  mockDeleteObject = jest.spyOn(utils, 'deleteObject')
  mockDeleteObject.mockImplementation(() =>
    Promise.resolve({
      $metadata: {
        httpStatusCode: 204,
      },
    })
  )

  mockDeleteObjects = jest.spyOn(utils, 'deleteObjects')
  mockDeleteObjects.mockImplementation(() =>
    Promise.resolve({
      $metadata: {
        httpStatusCode: 204,
      },
    })
  )

  mockHeadObject = jest.spyOn(utils, 'headObject')
  mockHeadObject.mockImplementation(() =>
    Promise.resolve({
      $metadata: {
        ContentLength: 1000,
      },
    })
  )
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
      url: '/object/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(mockGetObject).toBeCalled()
  })

  test('check if RLS policies are respected: anon user is not able to read authenticated resource', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/bucket2/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockGetObject).not.toHaveBeenCalled()
  })

  test('user is not able to read a resource without Auth header', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/bucket2/authenticated/casestudy.png',
    })
    expect(response.statusCode).toBe(400)
    expect(mockGetObject).not.toHaveBeenCalled()
  })

  test('return 400 when reading a non existent object', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/bucket2/authenticated/notfound',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockGetObject).not.toHaveBeenCalled()
  })

  test('return 400 when reading a non existent bucket', async () => {
    const response = await app().inject({
      method: 'GET',
      url: '/object/notfound/authenticated/casestudy.png',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockGetObject).not.toHaveBeenCalled()
  })
})
/*
 * POST /object/:id
 */
describe('testing POST object', () => {
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
    console.log(response)
    expect(response.statusCode).toBe(200)
    expect(mockUploadObject).toBeCalled()
    expect(response.body).toBe(
      `{"Key":"bjhaohmqunupljrqypxz/bucket2/authenticated/casestudy1.png"}`
    )
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
    expect(mockUploadObject).not.toHaveBeenCalled()
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
    expect(mockUploadObject).not.toHaveBeenCalled()
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
    expect(mockUploadObject).not.toHaveBeenCalled()
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
    expect(mockUploadObject).not.toHaveBeenCalled()
  })
})

/**
 * PUT /object/:id
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
    expect(mockUploadObject).toBeCalled()
    expect(response.body).toBe(`{"Key":"bjhaohmqunupljrqypxz/bucket2/authenticated/cat.jpg"}`)
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
    expect(mockUploadObject).not.toHaveBeenCalled()
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
    expect(mockUploadObject).not.toHaveBeenCalled()
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
    expect(mockUploadObject).not.toHaveBeenCalled()
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
    expect(mockUploadObject).not.toHaveBeenCalled()
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
        bucketName: 'bucket2',
        sourceKey: 'authenticated/casestudy.png',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(200)
    expect(mockCopyObject).toBeCalled()
    expect(response.body).toBe(`{"Key":"authenticated/casestudy11.png"}`)
  })

  test('check if RLS policies are respected: anon user is not able to update authenticated resource', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        bucketName: 'bucket2',
        sourceKey: 'authenticated/casestudy.png',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockCopyObject).not.toHaveBeenCalled()
  })

  test('user is not able to copy a resource without Auth header', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/copy',
      payload: {
        bucketName: 'bucket2',
        sourceKey: 'authenticated/casestudy.png',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockCopyObject).not.toHaveBeenCalled()
  })

  test('return 400 when copy from a non existent bucket', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        bucketName: 'notfound',
        sourceKey: 'authenticated/casestudy.png',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockCopyObject).not.toHaveBeenCalled()
  })

  test('return 400 when copying a non existent key', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/object/copy',
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
      payload: {
        bucketName: 'bucket2',
        sourceKey: 'authenticated/notfound.png',
        destinationKey: 'authenticated/casestudy11.png',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockCopyObject).not.toHaveBeenCalled()
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
    expect(mockDeleteObject).toBeCalled()
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
    expect(mockDeleteObject).not.toHaveBeenCalled()
  })

  test('user is not able to delete a resource without Auth header', async () => {
    const response = await app().inject({
      method: 'DELETE',
      url: '/object/bucket2/authenticated/delete1.png',
    })
    expect(response.statusCode).toBe(400)
    expect(mockDeleteObject).not.toHaveBeenCalled()
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
    expect(mockDeleteObject).not.toHaveBeenCalled()
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
    expect(mockDeleteObject).not.toHaveBeenCalled()
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
        prefixes: ['authenticated/delete-multiple1.png', 'authenticated/delete-multiple2.png'],
      },
    })
    expect(response.statusCode).toBe(200)
    expect(mockDeleteObjects).toBeCalled()

    const result = JSON.parse(response.body)
    console.log(response.body)
    expect(result[0].name).toBe('authenticated/delete-multiple1.png')
    expect(result[1].name).toBe('authenticated/delete-multiple2.png')
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
    expect(mockDeleteObjects).not.toHaveBeenCalled()
    const results = JSON.parse(response.body)
    expect(results.length).toBe(0)
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
    expect(mockDeleteObjects).not.toHaveBeenCalled()
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
    expect(mockDeleteObjects).not.toHaveBeenCalled()
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
    expect(mockDeleteObjects).not.toHaveBeenCalled()
    const results = JSON.parse(response.body)
    expect(results.length).toBe(0)
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
    expect(mockDeleteObjects).toBeCalled()
    const results = JSON.parse(response.body)
    expect(results.length).toBe(1)
    expect(results[0].name).toBe('authenticated/delete-multiple7.png')
  })
})

/**
 * POST /sign/
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
    console.log(response.body)
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
 * GET /sign/
 */
describe('testing retrieving signed URL', () => {
  test('get object with a token', async () => {
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const jwtToken = await signJWT({ url: urlToSign }, 100)
    const response = await app().inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${jwtToken}`,
    })
    expect(response.statusCode).toBe(200)
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
    console.log(response.body)
    expect(response.statusCode).toBe(400)
  })

  test('get object with an expired JWT', async () => {
    const urlToSign = 'bucket2/public/sadcat-upload.png'
    const expiredJWT = await signJWT({ url: urlToSign }, -1)
    const response = await app().inject({
      method: 'GET',
      url: `/object/sign/${urlToSign}?token=${expiredJWT}`,
    })
    console.log(response.body)
    expect(response.statusCode).toBe(400)
  })
})

describe('testing rename object', () => {
  test('check if RLS policies are respected: authenticated user is able to rename an authenticated object', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/object/rename`,
      payload: {
        sourceKey: 'authenticated/rename-orig.png',
        destinationKey: 'authenticated/rename-new.png',
        bucketName: 'bucket2',
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(200)
    expect(mockCopyObject).toHaveBeenCalled()
    expect(mockDeleteObject).toHaveBeenCalled()
  })

  test('check if RLS policies are respected: anon user is not able to rename an authenticated object', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/object/rename`,
      payload: {
        sourceKey: 'authenticated/rename-orig-2.png',
        destinationKey: 'authenticated/rename-new-2.png',
        bucketName: 'bucket2',
      },
      headers: {
        authorization: `Bearer ${anonKey}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockCopyObject).not.toHaveBeenCalled()
    expect(mockDeleteObject).not.toHaveBeenCalled()
  })

  test('user is not able to rename an object without auth header', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/object/rename`,
      payload: {
        sourceKey: 'authenticated/rename-orig-3.png',
        destinationKey: 'authenticated/rename-orig-new-3.png',
        bucketName: 'bucket2',
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockCopyObject).not.toHaveBeenCalled()
    expect(mockDeleteObject).not.toHaveBeenCalled()
  })

  test('user is not able to rename an object in a non existent bucket', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/object/rename`,
      payload: {
        sourceKey: 'authenticated/rename-orig-3.png',
        destinationKey: 'authenticated/rename-orig-new-3.png',
        bucketName: 'notfound',
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockCopyObject).not.toHaveBeenCalled()
    expect(mockDeleteObject).not.toHaveBeenCalled()
  })

  test('user is not able to rename an non existent object', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/object/rename`,
      payload: {
        sourceKey: 'authenticated/notfound',
        destinationKey: 'authenticated/rename-orig-new-3.png',
        bucketName: 'bucket2',
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockCopyObject).not.toHaveBeenCalled()
    expect(mockDeleteObject).not.toHaveBeenCalled()
  })

  test('user is not able to rename to an existing key', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/object/rename`,
      payload: {
        sourceKey: 'authenticated/rename-orig-2.png',
        destinationKey: 'authenticated/rename-orig-3.png',
        bucketName: 'bucket2',
      },
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
      },
    })
    expect(response.statusCode).toBe(400)
    expect(mockCopyObject).not.toHaveBeenCalled()
    expect(mockDeleteObject).not.toHaveBeenCalled()
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

  test('checking if RLS policies are respected', async () => {
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
