'use strict'
import { getConfig, mergeConfig } from '../config'

const { multitenantDatabaseUrl } = getConfig()
mergeConfig({
  pgQueueEnable: true,
  isMultitenant: true,
})

import dotenv from 'dotenv'
import * as migrate from '../internal/database/migrations/migrate'
import { multitenantKnex } from '../internal/database/multitenant-db'
import { adminApp } from './common'
import { s3CredentialsManager } from '@internal/database'
import { listenForTenantUpdate } from '@internal/database'
import { PostgresPubSub } from '@internal/pubsub'
import { encrypt, signJWT } from '@internal/auth'

dotenv.config({ path: '.env.test' })

const tenantId = 'abc123s3'

const pubSub = new PostgresPubSub(multitenantDatabaseUrl!)

// returns a promise that resolves the next time the jwk cache is invalidated
function createS3CredentialsChangeAwaiter(): Promise<string> {
  return new Promise<string>((resolve) => {
    pubSub.subscriber.notifications.once('tenants_s3_credentials_update', resolve)
  })
}

beforeAll(async () => {
  await migrate.runMultitenantMigrations()
  await pubSub.start()
  await listenForTenantUpdate(pubSub)
  jest.spyOn(migrate, 'runMigrationsOnTenant').mockResolvedValue()
})

beforeEach(async () => {
  const jwtSecret = 'zzzzzzzzzzz-s3'
  const serviceKey = await signJWT({}, jwtSecret, 100)
  await adminApp.inject({
    method: 'POST',
    url: `/tenants/${tenantId}`,
    payload: {
      anonKey: 'aaaaaaa',
      databaseUrl: 'bbbbbbb',
      jwtSecret,
      serviceKey,
    },
    headers: {
      apikey: process.env.ADMIN_API_KEYS,
    },
  })
})

afterEach(async () => {
  await adminApp.inject({
    method: 'DELETE',
    url: `/tenants/${tenantId}`,
    headers: {
      apikey: process.env.ADMIN_API_KEYS,
    },
  })
})

afterAll(async () => {
  await pubSub.close()
  await multitenantKnex.destroy()
})

describe('Tenant S3 credentials', () => {
  test('Add s3 credential without description', async () => {
    const response = await adminApp.inject({
      method: 'POST',
      url: `/s3/${tenantId}/credentials`,
      payload: {},
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(400)
  })

  test('Add s3 credential without claim', async () => {
    const response = await adminApp.inject({
      method: 'POST',
      url: `/s3/${tenantId}/credentials`,
      payload: { description: 'blah blah blah' },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(201)
    const createJson = await response.json()
    expect(Object.keys(createJson)).toHaveLength(4)
    expect(createJson.id).toBeTruthy()
    expect(createJson.description).toBeTruthy()
    expect(createJson.access_key).toBeTruthy()
    expect(createJson.secret_key).toBeTruthy()

    // check that item was added
    const getResponse = await adminApp.inject({
      method: 'GET',
      url: `/s3/${tenantId}/credentials`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(getResponse.statusCode).toBe(200)
    const getJson = await getResponse.json()
    expect(getJson).toHaveLength(1)
    expect(Object.keys(getJson[0])).toHaveLength(4)
    expect(getJson[0]).toMatchObject({
      id: createJson.id,
      description: createJson.description,
      access_key: createJson.access_key,
      created_at: expect.any(String),
    })
  })

  test('Add more than max allowed credentials', async () => {
    for (let i = 0; i < 50; i++) {
      const response = await adminApp.inject({
        method: 'POST',
        url: `/s3/${tenantId}/credentials`,
        payload: { description: 'blah blah blah' + i },
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(response.statusCode).toBe(201)
    }
    const responseFailure = await adminApp.inject({
      method: 'POST',
      url: `/s3/${tenantId}/credentials`,
      payload: { description: 'one too many' },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(responseFailure.statusCode).toBe(400)
  })

  test('Add s3 credential with claim', async () => {
    const knexTableSpy = jest.spyOn(multitenantKnex, 'table')
    try {
      const claimKept = {
        some: 'other',
        stuff: 'here',
        role: 'king of the world',
        sub: 'marine',
      }
      const claimRemoved = {
        iss: 'abc',
        exp: 54321,
        iat: 12345,
      }
      const claims = {
        issuer: 'def',
        ...claimRemoved,
        ...claimKept,
      }
      const response = await adminApp.inject({
        method: 'POST',
        url: `/s3/${tenantId}/credentials`,
        payload: { description: 'blah blah blah', claims },
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(response.statusCode).toBe(201)
      const createJson = await response.json()
      expect(Object.keys(createJson)).toHaveLength(4)
      expect(createJson.id).toBeTruthy()
      expect(createJson.description).toBeTruthy()
      expect(createJson.access_key).toBeTruthy()
      expect(createJson.secret_key).toBeTruthy()
      expect(knexTableSpy).toHaveBeenCalledTimes(2) // insert and count

      // check that the claims were stored correctly
      const keyResult = await s3CredentialsManager.getS3CredentialsByAccessKey(
        tenantId,
        createJson.access_key
      )
      // ensure it was loaded from the database
      expect(knexTableSpy).toHaveBeenCalledWith('tenants_s3_credentials')
      expect(knexTableSpy).toHaveBeenCalledTimes(3)
      expect(keyResult).toMatchObject({
        accessKey: createJson.access_key,
        secretKey: createJson.secret_key,
        claims: {
          issuer: `supabase.storage.${tenantId}`,
          ...claimKept,
        },
      })
      Object.keys(claimRemoved).forEach((k) => expect(k in keyResult.claims).toBe(false))

      // load again and ensure it was loaded from cache and not the database
      const cacheResult = await s3CredentialsManager.getS3CredentialsByAccessKey(
        tenantId,
        createJson.access_key
      )
      expect(knexTableSpy).toHaveBeenCalledTimes(3)
      expect(cacheResult).toMatchObject(keyResult)
    } finally {
      knexTableSpy.mockRestore()
    }
  })

  test('Delete s3 credential with missing payload', async () => {
    const deleteResponse = await adminApp.inject({
      method: 'DELETE',
      url: `/s3/${tenantId}/credentials`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(deleteResponse.statusCode).toBe(400)
  })

  test('Delete s3 credential with invalid id', async () => {
    const deleteResponse = await adminApp.inject({
      method: 'DELETE',
      url: `/s3/${tenantId}/credentials`,
      payload: { id: 'abc123' },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(deleteResponse.statusCode).toBe(400)
  })

  test('Delete s3 credential with not found id', async () => {
    const deleteResponse = await adminApp.inject({
      method: 'DELETE',
      url: `/s3/${tenantId}/credentials`,
      payload: { id: '59e0ddab-3e41-451c-bc42-f8bb1387381d' },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(deleteResponse.statusCode).toBe(204)
  })

  test('Delete s3 credential', async () => {
    const response = await adminApp.inject({
      method: 'POST',
      url: `/s3/${tenantId}/credentials`,
      payload: { description: 'blah blah blah' },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(response.statusCode).toBe(201)
    const createJson = await response.json()
    expect(Object.keys(createJson)).toHaveLength(4)
    expect(createJson.id).toBeTruthy()
    expect(createJson.description).toBeTruthy()
    expect(createJson.access_key).toBeTruthy()
    expect(createJson.secret_key).toBeTruthy()

    // check that item was added
    const getResponse = await adminApp.inject({
      method: 'GET',
      url: `/s3/${tenantId}/credentials`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(getResponse.statusCode).toBe(200)
    const getJson = await getResponse.json()
    expect(getJson).toHaveLength(1)
    expect(Object.keys(getJson[0])).toHaveLength(4)
    expect(getJson[0]).toMatchObject({
      id: createJson.id,
      description: createJson.description,
      access_key: createJson.access_key,
      created_at: expect.any(String),
    })

    // delete item
    const deleteResponse = await adminApp.inject({
      method: 'DELETE',
      url: `/s3/${tenantId}/credentials`,
      payload: { id: createJson.id },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(deleteResponse.statusCode).toBe(204)

    // check that item was deleted
    const getResponse2 = await adminApp.inject({
      method: 'GET',
      url: `/s3/${tenantId}/credentials`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
    expect(getResponse2.statusCode).toBe(200)
    const getJson2 = await getResponse2.json()
    expect(getJson2).toHaveLength(0)
  })

  test('Config always retrieves concurrent requests from cache', async () => {
    const getByKeySpy = jest.spyOn(s3CredentialsManager['storage'], 'getOneByAccessKey')
    try {
      const response = await adminApp.inject({
        method: 'POST',
        url: `/s3/${tenantId}/credentials`,
        payload: { description: 'blah blah blah' },
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(response.statusCode).toBe(201)
      const createJson = await response.json()

      const results = await Promise.all([
        s3CredentialsManager.getS3CredentialsByAccessKey(tenantId, createJson.access_key),
        s3CredentialsManager.getS3CredentialsByAccessKey(tenantId, createJson.access_key),
        s3CredentialsManager.getS3CredentialsByAccessKey(tenantId, createJson.access_key),
      ])
      expect(getByKeySpy).toHaveBeenCalledTimes(1)
      results.forEach((result, i) => expect(result).toEqual(results[i === 0 ? 1 : 0]))
      expect(results[0].accessKey).toBe(createJson.access_key)
    } finally {
      getByKeySpy.mockRestore()
    }
  })

  test('Ensure cache is cleared on delete', async () => {
    const knexTableSpy = jest.spyOn(multitenantKnex, 'table')
    const claims = {
      issuer: `supabase.storage.${tenantId}`,
      role: 'service_role',
    }
    try {
      const response = await adminApp.inject({
        method: 'POST',
        url: `/s3/${tenantId}/credentials`,
        payload: { description: 'blah blah blah' },
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(response.statusCode).toBe(201)
      const createJson = await response.json()
      expect(knexTableSpy).toHaveBeenCalledTimes(2) // create and count

      // check that the claims were stored correctly
      const keyResult = await s3CredentialsManager.getS3CredentialsByAccessKey(
        tenantId,
        createJson.access_key
      )
      // ensure it was loaded from the database
      expect(knexTableSpy).toHaveBeenCalledWith('tenants_s3_credentials')
      expect(knexTableSpy).toHaveBeenCalledTimes(3)
      expect(keyResult).toEqual({
        accessKey: createJson.access_key,
        secretKey: createJson.secret_key,
        claims,
      })

      // load again and ensure it was loaded from cache and not the database
      const cacheResult = await s3CredentialsManager.getS3CredentialsByAccessKey(
        tenantId,
        createJson.access_key
      )
      expect(knexTableSpy).toHaveBeenCalledTimes(3)
      expect(cacheResult).toEqual(keyResult)

      const configAwaiter = createS3CredentialsChangeAwaiter()

      // delete item
      const deleteResponse = await adminApp.inject({
        method: 'DELETE',
        url: `/s3/${tenantId}/credentials`,
        payload: { id: createJson.id },
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(deleteResponse.statusCode).toBe(204)
      expect(knexTableSpy).toHaveBeenCalledTimes(4)

      const cacheKey = await configAwaiter
      expect(cacheKey).toBe(tenantId + ':' + cacheResult.accessKey)

      // if cache is updated this should throw because it doesn't exist
      await expect(
        s3CredentialsManager.getS3CredentialsByAccessKey(tenantId, createJson.access_key)
      ).rejects.toThrow('The Access Key Id you provided does not exist in our records.')
      expect(knexTableSpy).toHaveBeenCalledWith('tenants_s3_credentials')
      expect(knexTableSpy).toHaveBeenCalledTimes(5)
    } finally {
      knexTableSpy.mockRestore()
    }
  })

  test('Ensure cache is cleared on update', async () => {
    const knexTableSpy = jest.spyOn(multitenantKnex, 'table')
    const claims = {
      issuer: `supabase.storage.${tenantId}`,
      role: 'service_role',
    }
    try {
      const response = await adminApp.inject({
        method: 'POST',
        url: `/s3/${tenantId}/credentials`,
        payload: { description: 'blah blah blah' },
        headers: {
          apikey: process.env.ADMIN_API_KEYS,
        },
      })
      expect(response.statusCode).toBe(201)
      const createJson = await response.json()
      expect(knexTableSpy).toHaveBeenCalledTimes(2) // create and count

      // check that the claims were stored correctly
      const keyResult = await s3CredentialsManager.getS3CredentialsByAccessKey(
        tenantId,
        createJson.access_key
      )
      // ensure it was loaded from the database
      expect(knexTableSpy).toHaveBeenCalledWith('tenants_s3_credentials')
      expect(knexTableSpy).toHaveBeenCalledTimes(3)
      expect(keyResult).toEqual({
        accessKey: createJson.access_key,
        secretKey: createJson.secret_key,
        claims,
      })

      // load again and ensure it was loaded from cache and not the database
      const cacheResult = await s3CredentialsManager.getS3CredentialsByAccessKey(
        tenantId,
        createJson.access_key
      )
      expect(knexTableSpy).toHaveBeenCalledTimes(3)
      expect(cacheResult).toEqual(keyResult)

      const configAwaiter = createS3CredentialsChangeAwaiter()

      // update item
      const secretKey = 'zzzzzzzzzzzzzzzzz'
      await multitenantKnex
        .table('tenants_s3_credentials')
        .update({ secret_key: encrypt(secretKey) })
        .where('id', createJson.id)
      expect(knexTableSpy).toHaveBeenCalledTimes(4)

      const cacheKey = await configAwaiter
      expect(cacheKey).toBe(tenantId + ':' + cacheResult.accessKey)

      // load again and ensure it was loaded from cache and not the database
      const cacheResult2 = await s3CredentialsManager.getS3CredentialsByAccessKey(
        tenantId,
        createJson.access_key
      )
      expect(knexTableSpy).toHaveBeenCalledWith('tenants_s3_credentials')
      expect(knexTableSpy).toHaveBeenCalledTimes(5)
      expect(cacheResult2).toEqual({ ...keyResult, secretKey })
    } finally {
      knexTableSpy.mockRestore()
    }
  })
})
