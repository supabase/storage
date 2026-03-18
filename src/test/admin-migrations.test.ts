jest.mock('@internal/database/migrations', () => {
  const actual = jest.requireActual('@internal/database/migrations')
  return {
    ...actual,
    resetMigrationsOnTenants: jest.fn(),
    resetMigration: jest.fn(),
    runMigrationsOnAllTenants: jest.fn(),
    runMigrationsOnTenant: jest.fn(),
  }
})

import * as migrations from '@internal/database/migrations'
import { DBMigration } from '@internal/database/migrations'
import { mergeConfig } from '../config'
import { multitenantKnex } from '../internal/database/multitenant-db'

mergeConfig({
  pgQueueEnable: true,
})

import { adminApp } from './common'

const tenantId = 'admin-migrations-test-tenant'

const tenantPayload = {
  anonKey: 'anon-key',
  databaseUrl: 'postgres://tenant-db',
  jwtSecret: 'jwt-secret',
  serviceKey: 'service-key',
}

describe('Admin migrations routes', () => {
  beforeAll(async () => {
    await migrations.runMultitenantMigrations()
  })

  afterEach(async () => {
    jest.clearAllMocks()

    await adminApp.inject({
      method: 'DELETE',
      url: `/tenants/${tenantId}`,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })
  })

  afterAll(async () => {
    await multitenantKnex.destroy()
  })

  test('rejects invalid markCompletedTillMigration for fleet reset', async () => {
    const resetSpy = jest.mocked(migrations.resetMigrationsOnTenants).mockResolvedValue(undefined)

    const response = await adminApp.inject({
      method: 'POST',
      url: '/migrations/reset/fleet',
      payload: {
        untilMigration: 'storage-schema' satisfies keyof typeof DBMigration,
        markCompletedTillMigration: 'not-a-real-migration',
      },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    expect(response.statusCode).toBe(400)
    expect(JSON.parse(response.body)).toEqual({ message: 'Invalid migration' })
    expect(resetSpy).not.toHaveBeenCalled()
  })

  test('rejects invalid markCompletedTillMigration for tenant reset', async () => {
    await adminApp.inject({
      method: 'POST',
      url: `/tenants/${tenantId}`,
      payload: tenantPayload,
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    const resetSpy = jest.mocked(migrations.resetMigration).mockResolvedValue(false)

    const response = await adminApp.inject({
      method: 'POST',
      url: `/tenants/${tenantId}/migrations/reset`,
      payload: {
        untilMigration: 'storage-schema' satisfies keyof typeof DBMigration,
        markCompletedTillMigration: 'not-a-real-migration',
      },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    expect(response.statusCode).toBe(400)
    expect(JSON.parse(response.body)).toEqual({ message: 'Invalid migration' })
    expect(resetSpy).not.toHaveBeenCalled()
  })

  test('accepts untilMigration that maps to numeric id 0 for fleet reset', async () => {
    const resetSpy = jest.mocked(migrations.resetMigrationsOnTenants).mockResolvedValue(undefined)

    const response = await adminApp.inject({
      method: 'POST',
      url: '/migrations/reset/fleet',
      payload: {
        untilMigration: 'create-migrations-table' satisfies keyof typeof DBMigration,
      },
      headers: {
        apikey: process.env.ADMIN_API_KEYS,
      },
    })

    expect(response.statusCode).toBe(200)
    expect(JSON.parse(response.body)).toEqual({ message: 'Migrations scheduled' })
    expect(resetSpy).toHaveBeenCalledWith({
      till: 'create-migrations-table',
      markCompletedTillMigration: undefined,
      signal: expect.any(AbortSignal),
    })
  })
})
