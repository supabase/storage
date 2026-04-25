import * as migrations from '@internal/database/migrations'
import { randomUUID } from 'crypto'
import type { FastifyInstance } from 'fastify'
import { multitenantKnex } from '../internal/database/multitenant-db'
import { PG_BOSS_SCHEMA } from '../internal/queue/queue'
import { createAdminApp } from './common'

const pgBossJobTable = `${PG_BOSS_SCHEMA}.job`
const pgBossBackupTable = `${PG_BOSS_SCHEMA}.job_overflow_backup`
const headers = {
  apikey: process.env.ADMIN_API_KEYS,
}

type SeedJobOptions = {
  eventType?: string
  id?: string
  name: string
  state?: string
  tenantRef?: string
}

let adminApp: FastifyInstance

async function seedJob(options: SeedJobOptions) {
  const id = options.id ?? randomUUID()
  const data: Record<string, unknown> = {}

  if (options.eventType) {
    data.event = {
      type: options.eventType,
    }
  }

  if (options.tenantRef) {
    data.tenant = {
      ref: options.tenantRef,
    }
  }

  await multitenantKnex(pgBossJobTable).insert({
    id,
    name: options.name,
    state: options.state ?? 'created',
    data,
  })

  return id
}

describe('Admin queue overflow routes', () => {
  beforeAll(async () => {
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
    await multitenantKnex.raw(`DROP TABLE IF EXISTS ${pgBossBackupTable}`)
    await multitenantKnex(pgBossJobTable).delete()
  })

  afterAll(async () => {
    await adminApp.close()
    await multitenantKnex.destroy()
  })

  it('returns summary counts for created jobs in the live queue table', async () => {
    await seedJob({
      id: '00000000-0000-0000-0000-000000000011',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: 'tenant-a',
    })
    await seedJob({
      id: '00000000-0000-0000-0000-000000000012',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: 'tenant-b',
    })
    await seedJob({
      id: '00000000-0000-0000-0000-000000000013',
      name: 'backup-object',
      tenantRef: 'tenant-a',
    })
    await seedJob({
      id: '00000000-0000-0000-0000-000000000014',
      name: 'webhooks',
      state: 'active',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: 'tenant-c',
    })

    const response = await adminApp.inject({
      method: 'GET',
      url: '/queue/overflow?limit=10',
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      backupTableExists: true,
      source: 'job',
      groupBy: 'summary',
      filters: {},
      data: [
        {
          count: 2,
          eventType: 'ObjectRemoved:Delete',
          name: 'webhooks',
        },
        {
          count: 1,
          eventType: null,
          name: 'backup-object',
        },
      ],
    })
  })

  it('returns tenant counts for a filtered queue and event type', async () => {
    await seedJob({
      id: '00000000-0000-0000-0000-000000000021',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: 'tenant-b',
    })
    await seedJob({
      id: '00000000-0000-0000-0000-000000000022',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: 'tenant-b',
    })
    await seedJob({
      id: '00000000-0000-0000-0000-000000000023',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: 'tenant-a',
    })
    await seedJob({
      id: '00000000-0000-0000-0000-000000000024',
      name: 'backup-object',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: 'tenant-z',
    })

    const response = await adminApp.inject({
      method: 'GET',
      url: '/queue/overflow?groupBy=tenant&name=webhooks&eventTypes=ObjectRemoved:Delete',
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      backupTableExists: true,
      source: 'job',
      groupBy: 'tenant',
      filters: {
        name: 'webhooks',
        eventTypes: ['ObjectRemoved:Delete'],
      },
      data: [
        {
          count: 2,
          tenantRef: 'tenant-b',
        },
        {
          count: 1,
          tenantRef: 'tenant-a',
        },
      ],
    })
  })

  it('returns an empty result when the overflow backup table does not exist', async () => {
    const response = await adminApp.inject({
      method: 'GET',
      url: '/queue/overflow?source=backup',
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      backupTableExists: false,
      source: 'backup',
      groupBy: 'summary',
      filters: {},
      data: [],
    })
  })

  it('backs up matching jobs into the overflow table', async () => {
    const movedJobId = await seedJob({
      id: '00000000-0000-0000-0000-000000000031',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: 'tenant-a',
    })
    await seedJob({
      id: '00000000-0000-0000-0000-000000000032',
      name: 'webhooks',
      eventType: 'ObjectCreated:Put',
      tenantRef: 'tenant-a',
    })
    await seedJob({
      id: '00000000-0000-0000-0000-000000000033',
      name: 'backup-object',
      tenantRef: 'tenant-a',
    })

    const response = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/backup',
      headers,
      payload: {
        name: 'webhooks',
        eventTypes: ['ObjectRemoved:Delete'],
      },
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      backupTableCreated: true,
      filters: {
        name: 'webhooks',
        eventTypes: ['ObjectRemoved:Delete'],
      },
      limit: null,
      movedCount: 1,
    })

    await expect(
      multitenantKnex(pgBossJobTable).where({ id: movedJobId }).first()
    ).resolves.toBeFalsy()
    await expect(
      multitenantKnex(pgBossBackupTable)
        .select('id', 'name', 'state')
        .where({ id: movedJobId })
        .first()
    ).resolves.toEqual({
      id: movedJobId,
      name: 'webhooks',
      state: 'created',
    })
  })

  it('restores backed up jobs in batches', async () => {
    await seedJob({
      id: '00000000-0000-0000-0000-000000000041',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: 'tenant-a',
    })
    await seedJob({
      id: '00000000-0000-0000-0000-000000000042',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: 'tenant-b',
    })

    const backupResponse = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/backup',
      headers,
      payload: {
        name: 'webhooks',
      },
    })

    expect(backupResponse.statusCode).toBe(200)

    const listResponse = await adminApp.inject({
      method: 'GET',
      url: '/queue/overflow?source=backup&name=webhooks',
      headers,
    })

    expect(listResponse.statusCode).toBe(200)
    expect(listResponse.json()).toEqual({
      backupTableExists: true,
      source: 'backup',
      groupBy: 'summary',
      filters: {
        name: 'webhooks',
      },
      data: [
        {
          count: 2,
          eventType: 'ObjectRemoved:Delete',
          name: 'webhooks',
        },
      ],
    })

    const restoreResponse = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/restore',
      headers,
      payload: {
        name: 'webhooks',
        limit: 1,
      },
    })

    expect(restoreResponse.statusCode).toBe(200)
    expect(restoreResponse.json()).toEqual({
      backupTableExists: true,
      filters: {
        name: 'webhooks',
      },
      limit: 1,
      movedCount: 1,
    })

    await expect(
      multitenantKnex(pgBossJobTable).count<{ count: number }[]>('* AS count')
    ).resolves.toEqual([
      {
        count: 1,
      },
    ])
    await expect(
      multitenantKnex(pgBossBackupTable).count<{ count: number }[]>('* AS count')
    ).resolves.toEqual([
      {
        count: 1,
      },
    ])
    await expect(
      multitenantKnex(pgBossJobTable)
        .select('id')
        .where({ id: '00000000-0000-0000-0000-000000000041' })
        .first()
    ).resolves.toEqual({
      id: '00000000-0000-0000-0000-000000000041',
    })
  })
})
