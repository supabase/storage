vi.mock('@internal/database/migrations', async () => {
  const actual = await vi.importActual<typeof import('@internal/database/migrations')>(
    '@internal/database/migrations'
  )
  return {
    ...actual,
    resetMigrationsOnTenants: vi.fn(),
    resetMigration: vi.fn(),
    runMigrationsOnAllTenants: vi.fn(),
    runMigrationsOnTenant: vi.fn(),
  }
})

import * as migrations from '@internal/database/migrations'
import { DBMigration } from '@internal/database/migrations'
import { randomUUID } from 'crypto'
import type { FastifyInstance } from 'fastify'
import { mergeConfig } from '../config'
import { multitenantKnex } from '../internal/database/multitenant-db'
import { PG_BOSS_SCHEMA, Queue } from '../internal/queue/queue'
import { RunMigrationsOnTenants } from '../storage/events/migrations/run-migrations'
import { createAdminApp } from './common'

const tenantId = 'admin-migrations-test-tenant'
const createdJobIds = new Set<string>()
const createdTenantIds = new Set<string>()
const pgBossJobTable = `${PG_BOSS_SCHEMA}.job`
const headers = {
  apikey: process.env.ADMIN_API_KEYS,
}

const tenantPayload = {
  anonKey: 'anon-key',
  databaseUrl: 'postgres://tenant-db',
  jwtSecret: 'jwt-secret',
  serviceKey: 'service-key',
}

function trackJobId(jobId: string) {
  createdJobIds.add(jobId)
  return jobId
}

let adminApp: FastifyInstance

async function createTenant(currentTenantId: string) {
  createdTenantIds.add(currentTenantId)
  const response = await adminApp.inject({
    method: 'POST',
    url: `/tenants/${currentTenantId}`,
    payload: tenantPayload,
    headers,
  })

  expect(response.statusCode).toBe(201)
}

describe('Admin migrations routes', () => {
  beforeAll(async () => {
    mergeConfig({
      pgQueueEnable: true,
    })
    await migrations.runMultitenantMigrations()
    await multitenantKnex.raw(`CREATE SCHEMA IF NOT EXISTS ${PG_BOSS_SCHEMA}`)
    await multitenantKnex.raw(`
      CREATE TABLE IF NOT EXISTS ${pgBossJobTable} (
        id uuid PRIMARY KEY,
        name text NOT NULL,
        state text NOT NULL,
        created_on timestamptz NOT NULL DEFAULT now(),
        data jsonb NOT NULL DEFAULT '{}'::jsonb
      )
    `)
    adminApp = await createAdminApp()
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    vi.clearAllMocks()

    if (createdJobIds.size > 0) {
      await multitenantKnex(pgBossJobTable).whereIn('id', Array.from(createdJobIds)).delete()
      createdJobIds.clear()
    }

    const tenantIdsToDelete = new Set([tenantId, ...createdTenantIds])
    createdTenantIds.clear()

    for (const currentTenantId of tenantIdsToDelete) {
      await adminApp.inject({
        method: 'DELETE',
        url: `/tenants/${currentTenantId}`,
        headers,
      })
    }
  })

  afterAll(async () => {
    await adminApp?.close()
    await multitenantKnex.destroy()
  })

  test('rejects invalid markCompletedTillMigration for fleet reset', async () => {
    const resetSpy = vi.mocked(migrations.resetMigrationsOnTenants).mockResolvedValue(undefined)

    const response = await adminApp.inject({
      method: 'POST',
      url: '/migrations/reset/fleet',
      payload: {
        untilMigration: 'storage-schema' satisfies keyof typeof DBMigration,
        markCompletedTillMigration: 'not-a-real-migration',
      },
      headers,
    })

    expect(response.statusCode).toBe(400)
    expect(JSON.parse(response.body)).toEqual({ message: 'Invalid migration' })
    expect(resetSpy).not.toHaveBeenCalled()
  })

  test('rejects invalid markCompletedTillMigration for tenant reset', async () => {
    await createTenant(tenantId)

    const resetSpy = vi.mocked(migrations.resetMigration).mockResolvedValue(false)

    const response = await adminApp.inject({
      method: 'POST',
      url: `/tenants/${tenantId}/migrations/reset`,
      payload: {
        untilMigration: 'storage-schema' satisfies keyof typeof DBMigration,
        markCompletedTillMigration: 'not-a-real-migration',
      },
      headers,
    })

    expect(response.statusCode).toBe(400)
    expect(JSON.parse(response.body)).toEqual({ message: 'Invalid migration' })
    expect(resetSpy).not.toHaveBeenCalled()
  })

  test('accepts untilMigration that maps to numeric id 0 for fleet reset', async () => {
    const resetSpy = vi.mocked(migrations.resetMigrationsOnTenants).mockResolvedValue(undefined)

    const response = await adminApp.inject({
      method: 'POST',
      url: '/migrations/reset/fleet',
      payload: {
        untilMigration: 'create-migrations-table' satisfies keyof typeof DBMigration,
      },
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(JSON.parse(response.body)).toEqual({ message: 'Migrations scheduled' })
    expect(resetSpy).toHaveBeenCalledWith({
      till: 'create-migrations-table',
      markCompletedTillMigration: undefined,
      signal: expect.any(AbortSignal),
    })
  })

  test('manual tenant migration updates the tenant migration state', async () => {
    const migrationTenantId = `admin-migrations-state-${randomUUID().slice(0, 8)}`
    const latestMigration = await migrations.lastLocalMigrationName()

    await createTenant(migrationTenantId)

    await multitenantKnex('tenants').where({ id: migrationTenantId }).update({
      migrations_version: null,
      migrations_status: null,
    })

    const migrateResponse = await adminApp.inject({
      method: 'POST',
      url: `/tenants/${migrationTenantId}/migrations`,
      headers,
    })

    expect(migrateResponse.statusCode).toBe(200)
    expect(JSON.parse(migrateResponse.body)).toEqual({ migrated: true })

    const getResponse = await adminApp.inject({
      method: 'GET',
      url: `/tenants/${migrationTenantId}/migrations`,
      headers,
    })

    expect(getResponse.statusCode).toBe(200)
    expect(JSON.parse(getResponse.body)).toEqual({
      isLatest: true,
      migrationsVersion: latestMigration,
      migrationsStatus: 'COMPLETED',
    })

    await expect(
      multitenantKnex('tenants')
        .select('migrations_version', 'migrations_status')
        .where({ id: migrationTenantId })
        .first()
    ).resolves.toEqual({
      migrations_version: latestMigration,
      migrations_status: 'COMPLETED',
    })
  })

  test('manual tenant migration records the frozen migration target', async () => {
    const migrationTenantId = `admin-migrations-freeze-${randomUUID().slice(0, 8)}`
    const frozenMigration = 'create-migrations-table' satisfies keyof typeof DBMigration
    let isolatedAdminApp: FastifyInstance | undefined
    let isolatedMultitenantKnex: typeof multitenantKnex | undefined

    await createTenant(migrationTenantId)

    await multitenantKnex('tenants').where({ id: migrationTenantId }).update({
      migrations_version: null,
      migrations_status: null,
    })

    vi.resetModules()

    try {
      const config = await import('../config')
      config.getConfig({ reload: true })
      config.mergeConfig({
        pgQueueEnable: true,
        dbMigrationFreezeAt: frozenMigration,
      })

      const isolatedMigrations = await import('@internal/database/migrations')
      vi.mocked(isolatedMigrations.runMigrationsOnTenant).mockResolvedValue(undefined)

      const multitenantDb = await import('../internal/database/multitenant-db')
      isolatedMultitenantKnex = multitenantDb.multitenantKnex

      const adminAppModule = await import('../admin-app')
      isolatedAdminApp = adminAppModule.default({})

      if (!isolatedAdminApp) {
        throw new Error('Failed to build isolated admin app')
      }

      const migrateResponse = await isolatedAdminApp.inject({
        method: 'POST',
        url: `/tenants/${migrationTenantId}/migrations`,
        headers,
      })

      expect(migrateResponse.statusCode).toBe(200)
      expect(JSON.parse(migrateResponse.body)).toEqual({ migrated: true })

      const getResponse = await isolatedAdminApp.inject({
        method: 'GET',
        url: `/tenants/${migrationTenantId}/migrations`,
        headers,
      })

      expect(getResponse.statusCode).toBe(200)
      expect(JSON.parse(getResponse.body)).toEqual({
        isLatest: true,
        migrationsVersion: frozenMigration,
        migrationsStatus: 'COMPLETED',
      })

      await expect(
        multitenantKnex('tenants')
          .select('migrations_version', 'migrations_status')
          .where({ id: migrationTenantId })
          .first()
      ).resolves.toEqual({
        migrations_version: frozenMigration,
        migrations_status: 'COMPLETED',
      })
    } finally {
      await isolatedAdminApp?.close()
      if (isolatedMultitenantKnex && isolatedMultitenantKnex !== multitenantKnex) {
        await isolatedMultitenantKnex.destroy()
      }
      vi.resetModules()
    }
  })

  test('lists active fleet migration jobs from the current pg-boss queue name', async () => {
    const fleetTenantId = `admin-migrations-fleet-${randomUUID().slice(0, 8)}`
    const jobId = trackJobId(randomUUID())
    const wrongQueueJobId = trackJobId(randomUUID())
    const wrongStateJobId = trackJobId(randomUUID())

    await createTenant(fleetTenantId)

    await multitenantKnex(pgBossJobTable).insert({
      id: jobId,
      name: RunMigrationsOnTenants.getQueueName(),
      state: 'active',
      data: {
        tenantId: fleetTenantId,
      },
    })
    await multitenantKnex(pgBossJobTable).insert({
      id: wrongQueueJobId,
      name: 'another-queue',
      state: 'active',
      data: {
        tenantId: fleetTenantId,
      },
    })
    await multitenantKnex(pgBossJobTable).insert({
      id: wrongStateJobId,
      name: RunMigrationsOnTenants.getQueueName(),
      state: 'created',
      data: {
        tenantId: fleetTenantId,
      },
    })

    const response = await adminApp.inject({
      method: 'GET',
      url: '/migrations/active',
      headers,
    })

    expect(response.statusCode).toBe(200)
    const body = JSON.parse(response.body)
    expect(body).toHaveLength(1)
    expect(body).toEqual([
      expect.objectContaining({
        id: jobId,
        name: RunMigrationsOnTenants.getQueueName(),
        state: 'active',
      }),
    ])
  })

  test('lists tenant migration jobs from the current pg-boss schema and queue name', async () => {
    const jobTenantId = `admin-migrations-tenant-${randomUUID().slice(0, 8)}`
    const otherTenantId = `admin-migrations-other-${randomUUID().slice(0, 8)}`
    const olderJobId = trackJobId(randomUUID())
    const newerJobId = trackJobId(randomUUID())
    const otherTenantJobId = trackJobId(randomUUID())
    const wrongQueueJobId = trackJobId(randomUUID())

    await createTenant(jobTenantId)
    await createTenant(otherTenantId)

    await multitenantKnex(pgBossJobTable).insert({
      id: olderJobId,
      name: RunMigrationsOnTenants.getQueueName(),
      state: 'active',
      created_on: new Date('2026-03-25T10:00:00.000Z'),
      data: {
        tenant: {
          ref: jobTenantId,
        },
        tenantId: jobTenantId,
      },
    })
    await multitenantKnex(pgBossJobTable).insert({
      id: newerJobId,
      name: RunMigrationsOnTenants.getQueueName(),
      state: 'active',
      created_on: new Date('2026-03-25T11:00:00.000Z'),
      data: {
        tenant: {
          ref: jobTenantId,
        },
        tenantId: jobTenantId,
      },
    })
    await multitenantKnex(pgBossJobTable).insert({
      id: otherTenantJobId,
      name: RunMigrationsOnTenants.getQueueName(),
      state: 'active',
      data: {
        tenant: {
          ref: otherTenantId,
        },
        tenantId: otherTenantId,
      },
    })
    await multitenantKnex(pgBossJobTable).insert({
      id: wrongQueueJobId,
      name: 'another-queue',
      state: 'active',
      data: {
        tenant: {
          ref: jobTenantId,
        },
        tenantId: jobTenantId,
      },
    })

    const response = await adminApp.inject({
      method: 'GET',
      url: `/tenants/${jobTenantId}/migrations/jobs`,
      headers,
    })

    expect(response.statusCode).toBe(200)
    const body = JSON.parse(response.body)
    expect(body).toHaveLength(2)
    expect(body).toEqual([
      expect.objectContaining({
        id: newerJobId,
        name: RunMigrationsOnTenants.getQueueName(),
      }),
      expect.objectContaining({
        id: olderJobId,
        name: RunMigrationsOnTenants.getQueueName(),
      }),
    ])
  })

  test('returns queue progress for the current migration queue', async () => {
    const getQueueSize = vi.fn().mockResolvedValue(7)
    vi.spyOn(Queue, 'getInstance').mockReturnValue({
      getQueueSize,
    } as never)

    const response = await adminApp.inject({
      method: 'GET',
      url: '/migrations/progress',
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(JSON.parse(response.body)).toEqual({ remaining: 7 })
    expect(getQueueSize).toHaveBeenCalledWith(RunMigrationsOnTenants.getQueueName())
  })

  test('lists failed tenants and paginates by cursor', async () => {
    const firstFailedTenantId = `admin-migrations-failed-${randomUUID().slice(0, 8)}`
    const secondFailedTenantId = `admin-migrations-failed-${randomUUID().slice(0, 8)}`
    const healthyTenantId = `admin-migrations-ok-${randomUUID().slice(0, 8)}`

    await createTenant(firstFailedTenantId)
    await createTenant(secondFailedTenantId)
    await createTenant(healthyTenantId)

    await multitenantKnex('tenants').where({ id: firstFailedTenantId }).update({
      migrations_status: 'FAILED',
    })
    await multitenantKnex('tenants').where({ id: secondFailedTenantId }).update({
      migrations_status: 'FAILED',
    })
    await multitenantKnex('tenants').where({ id: healthyTenantId }).update({
      migrations_status: 'COMPLETED',
    })

    const firstPageResponse = await adminApp.inject({
      method: 'GET',
      url: '/migrations/failed',
      headers,
    })

    expect(firstPageResponse.statusCode).toBe(200)
    const firstPageBody = JSON.parse(firstPageResponse.body)
    expect(firstPageBody.data).toHaveLength(2)
    expect(firstPageBody.data.map((tenant: { id: string }) => tenant.id)).toEqual([
      firstFailedTenantId,
      secondFailedTenantId,
    ])

    const secondPageResponse = await adminApp.inject({
      method: 'GET',
      url: `/migrations/failed?cursor=${firstPageBody.data[0].cursor_id}`,
      headers,
    })

    expect(secondPageResponse.statusCode).toBe(200)
    expect(JSON.parse(secondPageResponse.body)).toEqual({
      next_cursor_id: firstPageBody.data[1].cursor_id,
      data: [
        expect.objectContaining({
          id: secondFailedTenantId,
          cursor_id: firstPageBody.data[1].cursor_id,
        }),
      ],
    })
  })

  test('marks only active fleet migration jobs from the current queue as completed', async () => {
    const matchingJobId = trackJobId(randomUUID())
    const wrongQueueJobId = trackJobId(randomUUID())
    const wrongStateJobId = trackJobId(randomUUID())

    await multitenantKnex(pgBossJobTable).insert([
      {
        id: matchingJobId,
        name: RunMigrationsOnTenants.getQueueName(),
        state: 'active',
        data: {},
      },
      {
        id: wrongQueueJobId,
        name: 'another-queue',
        state: 'active',
        data: {},
      },
      {
        id: wrongStateJobId,
        name: RunMigrationsOnTenants.getQueueName(),
        state: 'created',
        data: {},
      },
    ])

    const response = await adminApp.inject({
      method: 'DELETE',
      url: '/migrations/active',
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(JSON.parse(response.body)).toBe(1)

    const rows = await multitenantKnex(pgBossJobTable)
      .select('id', 'state')
      .whereIn('id', [matchingJobId, wrongQueueJobId, wrongStateJobId])
    const statesById = Object.fromEntries(rows.map((row) => [row.id, row.state]))

    expect(statesById).toEqual({
      [matchingJobId]: 'completed',
      [wrongQueueJobId]: 'active',
      [wrongStateJobId]: 'created',
    })
  })

  test('deletes only tenant migration jobs from the current queue', async () => {
    const tenantWithJobsId = `admin-migrations-delete-${randomUUID().slice(0, 8)}`
    const otherTenantId = `admin-migrations-delete-other-${randomUUID().slice(0, 8)}`
    const matchingJobId = trackJobId(randomUUID())
    const otherTenantJobId = trackJobId(randomUUID())
    const wrongQueueJobId = trackJobId(randomUUID())

    await createTenant(tenantWithJobsId)
    await createTenant(otherTenantId)

    await multitenantKnex(pgBossJobTable).insert([
      {
        id: matchingJobId,
        name: RunMigrationsOnTenants.getQueueName(),
        state: 'active',
        data: {
          tenant: {
            ref: tenantWithJobsId,
          },
          tenantId: tenantWithJobsId,
        },
      },
      {
        id: otherTenantJobId,
        name: RunMigrationsOnTenants.getQueueName(),
        state: 'active',
        data: {
          tenant: {
            ref: otherTenantId,
          },
          tenantId: otherTenantId,
        },
      },
      {
        id: wrongQueueJobId,
        name: 'another-queue',
        state: 'active',
        data: {
          tenant: {
            ref: tenantWithJobsId,
          },
          tenantId: tenantWithJobsId,
        },
      },
    ])

    const response = await adminApp.inject({
      method: 'DELETE',
      url: `/tenants/${tenantWithJobsId}/migrations/jobs`,
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(JSON.parse(response.body)).toBe(1)

    const rows = await multitenantKnex(pgBossJobTable)
      .select('id', 'name')
      .whereIn('id', [matchingJobId, otherTenantJobId, wrongQueueJobId])
    const namesById = Object.fromEntries(rows.map((row) => [row.id, row.name]))

    expect(namesById).toEqual({
      [otherTenantJobId]: RunMigrationsOnTenants.getQueueName(),
      [wrongQueueJobId]: 'another-queue',
    })
  })

  test('returns 0 when deleting tenant migration jobs for a tenant with no matching jobs', async () => {
    const tenantWithoutJobsId = `admin-migrations-empty-${randomUUID().slice(0, 8)}`

    await createTenant(tenantWithoutJobsId)

    const response = await adminApp.inject({
      method: 'DELETE',
      url: `/tenants/${tenantWithoutJobsId}/migrations/jobs`,
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(JSON.parse(response.body)).toBe(0)
  })
})
