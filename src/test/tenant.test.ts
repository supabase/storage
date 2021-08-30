'use strict'
import app from '../admin-app'
import dotenv from 'dotenv'
import * as migrate from '../utils/migrate'
import { pool } from '../utils/multitenant-db'

dotenv.config({ path: '.env.test' })

const payload = {
  anonKey: 'a',
  databaseUrl: 'b',
  jwtSecret: 'c',
  serviceKey: 'd',
}

const payload2 = {
  anonKey: 'e',
  databaseUrl: 'f',
  jwtSecret: 'g',
  serviceKey: 'h',
}

beforeAll(async () => {
  await migrate.runMultitenantMigrations()
  jest.spyOn(migrate, 'runMigrationsOnTenant').mockResolvedValue()
})

afterEach(async () => {
  await app().inject({
    method: 'DELETE',
    url: '/tenants/abc',
    headers: {
      apikey: process.env.ADMIN_API_KEY,
    },
  })
})

afterAll(async () => {
  await pool.end()
})

describe('Tenant configs', () => {
  test('Get all tenant configs', async () => {
    await app().inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    const response = await app().inject({
      method: 'GET',
      url: `/tenants`,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toEqual([
      {
        id: 'abc',
        ...payload,
      },
    ])
  })

  test('Get nonexistent tenant config', async () => {
    const response = await app().inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(response.statusCode).toBe(404)
  })

  test('Get existing tenant config', async () => {
    await app().inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    const response = await app().inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON).toEqual(payload)
  })

  test('Insert tenant config without required properties', async () => {
    const response = await app().inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload: {},
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('Insert tenant config twice', async () => {
    const firstInsertResponse = await app().inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(firstInsertResponse.statusCode).toBe(201)
    const secondInsertResponse = await app().inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(secondInsertResponse.statusCode).toBe(500)
  })

  test('Update tenant config', async () => {
    await app().inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    const patchResponse = await app().inject({
      method: 'PATCH',
      url: `/tenants/abc`,
      payload: payload2,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(patchResponse.statusCode).toBe(204)
    const getResponse = await app().inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    const getResponseJSON = JSON.parse(getResponse.body)
    expect(getResponseJSON).toEqual(payload2)
  })

  test('Upsert tenant config', async () => {
    const firstPutResponse = await app().inject({
      method: 'PUT',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(firstPutResponse.statusCode).toBe(204)
    const firstGetResponse = await app().inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    const firstGetResponseJSON = JSON.parse(firstGetResponse.body)
    expect(firstGetResponseJSON).toEqual(payload)
    const secondPutResponse = await app().inject({
      method: 'PUT',
      url: `/tenants/abc`,
      payload: payload2,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(secondPutResponse.statusCode).toBe(204)
    const secondGetResponse = await app().inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    const secondGetResponseJSON = JSON.parse(secondGetResponse.body)
    expect(secondGetResponseJSON).toEqual(payload2)
  })

  test('Delete tenant config', async () => {
    await app().inject({
      method: 'POST',
      url: `/tenants/abc`,
      payload,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    const deleteResponse = await app().inject({
      method: 'DELETE',
      url: '/tenants/abc',
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(deleteResponse.statusCode).toBe(204)
    const getResponse = await app().inject({
      method: 'GET',
      url: `/tenants/abc`,
      headers: {
        apikey: process.env.ADMIN_API_KEY,
      },
    })
    expect(getResponse.statusCode).toBe(404)
  })
})
