'use strict'
import app from '../app'
import { getConfig } from '../utils/config'
import dotenv from 'dotenv'

dotenv.config({ path: '.env.test' })
const { anonKey, serviceKey } = getConfig()

beforeEach(() => {
  jest.clearAllMocks()
})

describe('testing search', () => {
  test('searching the bucket root folder', async () => {
    const response = await app().inject({
      method: 'POST',
      url: '/search/bucket2',
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
      url: '/search/bucket2',
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
      url: '/search/bucket2',
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
      url: '/search/bucket2',
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
      url: '/search/bucket2',
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
      url: '/search/bucket2',
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
      url: '/search/bucket2',
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
      url: '/search/bucket2',
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
      url: '/search/bucket2',
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
