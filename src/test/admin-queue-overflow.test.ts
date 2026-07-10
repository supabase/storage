import { randomUUID } from 'node:crypto'
import * as migrations from '@internal/database/migrations'
import type { FastifyInstance } from 'fastify'
import pg from 'pg'
import PgBoss from 'pg-boss'
import { getConfig, mergeConfig } from '../config'
import { closeMultitenantPg, type DatabaseExecutor } from '../internal/database'
import { PgPoolExecutor } from '../internal/database/pg-connection'
import { PG_BOSS_SCHEMA, Queue, QueueOverflowStorePg } from '../internal/queue'
import { QueueDB } from '../internal/queue/database'
import { createAdminApp } from './common'

const pgBossJobTable = `${PG_BOSS_SCHEMA}.job`
const pgBossBackupTable = `${PG_BOSS_SCHEMA}.job_overflow_backup`
const testRunId = randomUUID()
const headers = {
  apikey: process.env.ADMIN_API_KEYS,
}
const pgIt = process.env.TEST_DATABASE_ACCESS_METHOD === 'orioledb' ? it.skip : it

type SeedJobOptions = {
  eventType?: string
  id?: string
  name: string
  state?: string
  tenantRef?: string
}

type JobRow = {
  id: string
  name: string
  state: string
}

let adminApp: FastifyInstance
let queueDbSpy: ReturnType<typeof vi.spyOn>
let routeBoss: PgBoss | undefined
let routeQueueDb: QueueDB | undefined
let queryPool: pg.Pool | undefined
let queryDb: PgPoolExecutor | undefined

function resolveQueueTestConnectionString(config: ReturnType<typeof getConfig>) {
  if (config.pgQueueConnectionURL) {
    return config.pgQueueConnectionURL
  }

  if (config.isMultitenant) {
    return config.multitenantDatabasePoolUrl || config.multitenantDatabaseUrl
  }

  return config.databaseURL
}

async function seedJob(options: SeedJobOptions) {
  if (!queryDb) {
    throw new Error('Queue test database is not initialized')
  }

  const id = options.id ?? randomUUID()
  const data: Record<string, unknown> = {
    queueOverflowTest: testRunId,
  }

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

  await queryDb.query({
    text: `
      INSERT INTO ${pgBossJobTable} (id, name, state, data)
      VALUES ($1, $2, $3, $4::jsonb)
    `,
    values: [id, options.name, options.state ?? 'created', JSON.stringify(data)],
  })

  return id
}

async function findJob(
  table: string,
  name: string,
  id: string,
  db: DatabaseExecutor | undefined = queryDb
): Promise<JobRow | undefined> {
  if (!db) {
    throw new Error('Queue test database is not initialized')
  }

  const result = await db.query<JobRow>({
    text: `
      SELECT id, name, state
      FROM ${table}
      WHERE name = $1 AND id = $2
      LIMIT 1
    `,
    values: [name, id],
  })

  return result.rows[0]
}

async function findJobData(
  table: string,
  name: string,
  id: string,
  db: DatabaseExecutor | undefined = queryDb
): Promise<Record<string, unknown> | undefined> {
  if (!db) {
    throw new Error('Queue test database is not initialized')
  }

  const result = await db.query<{ data: Record<string, unknown> }>({
    text: `
      SELECT data
      FROM ${table}
      WHERE name = $1 AND id = $2
      LIMIT 1
    `,
    values: [name, id],
  })

  return result.rows[0]?.data
}

async function cleanupTestJobs(db: DatabaseExecutor | undefined = queryDb) {
  if (!db) {
    return
  }

  const jobTable = await db.query<{ table_name: string | null }>({
    text: 'SELECT to_regclass($1) AS table_name',
    values: [pgBossJobTable],
  })

  if (!jobTable.rows[0]?.table_name) {
    return
  }

  await db.query(`DROP TABLE IF EXISTS ${pgBossBackupTable}`)
  await db.query({
    text: `DELETE FROM ${pgBossJobTable} WHERE data->>'queueOverflowTest' = $1`,
    values: [testRunId],
  })
}

describe('Admin queue overflow routes', () => {
  beforeAll(async () => {
    getConfig()
    mergeConfig({ pgQueueEnable: true })
    await migrations.runMultitenantMigrations()
    const config = getConfig()
    const connectionString = resolveQueueTestConnectionString(config)

    if (!connectionString) {
      throw new Error('Expected a multitenant database URL for queue overflow integration tests')
    }

    const inspectionPool = new pg.Pool({
      application_name: config.databaseApplicationName,
      connectionString,
      connectionTimeoutMillis: config.databaseConnectionTimeout,
      max: 1,
      statement_timeout: config.pgQueueReadWriteTimeout,
    })
    const inspectionDb = new PgPoolExecutor(inspectionPool)
    const installation = await inspectionDb.query<{
      job_table: string | null
      version_table: string | null
    }>({
      text: `
        SELECT
          to_regclass($1) AS job_table,
          to_regclass($2) AS version_table
      `,
      values: [`${PG_BOSS_SCHEMA}.job`, `${PG_BOSS_SCHEMA}.version`],
    })

    if (installation.rows[0]?.job_table && !installation.rows[0]?.version_table) {
      await inspectionDb.query(`DROP SCHEMA ${PG_BOSS_SCHEMA} CASCADE`)
    }
    await inspectionPool.end()

    queryPool = new pg.Pool({
      application_name: config.databaseApplicationName,
      connectionString,
      connectionTimeoutMillis: config.databaseConnectionTimeout,
      max: 1,
      statement_timeout: config.pgQueueReadWriteTimeout,
    })
    queryDb = new PgPoolExecutor(queryPool)

    routeQueueDb = new QueueDB({
      application_name: config.databaseApplicationName,
      connectionString,
      connectionTimeoutMillis: config.databaseConnectionTimeout,
      max: 2,
      statement_timeout: config.pgQueueReadWriteTimeout,
    })
    routeBoss = new PgBoss({
      connectionString,
      db: routeQueueDb,
      schema: PG_BOSS_SCHEMA,
      schedule: false,
      supervise: false,
    })
    await routeBoss.start()
    await routeBoss.createQueue('webhooks', { name: 'webhooks', policy: 'standard' })
    await routeBoss.createQueue('backup-object', { name: 'backup-object', policy: 'standard' })
    await cleanupTestJobs()
    queueDbSpy = vi.spyOn(Queue, 'getDb').mockReturnValue(routeQueueDb)
    adminApp = await createAdminApp()
  })

  afterEach(async () => {
    await cleanupTestJobs()
  })

  afterAll(async () => {
    await adminApp?.close()
    queueDbSpy?.mockRestore()
    await cleanupTestJobs()
    await routeBoss?.stop({ graceful: false, wait: true }).catch(() => routeQueueDb?.close())
    await queryPool?.end()
    await closeMultitenantPg()
  })

  it('requires the admin API key', async () => {
    const response = await adminApp.inject({
      method: 'GET',
      url: '/queue/overflow',
    })

    expect(response.statusCode).toBe(401)
  })

  it('returns exact summary totals and truncation metadata for created jobs', async () => {
    const tenantA = `${testRunId}-tenant-a`
    const tenantB = `${testRunId}-tenant-b`
    const tenantC = `${testRunId}-tenant-c`
    await seedJob({ name: 'webhooks', eventType: 'ObjectRemoved:Delete', tenantRef: tenantA })
    await seedJob({ name: 'webhooks', eventType: 'ObjectRemoved:Delete', tenantRef: tenantB })
    await seedJob({ name: 'backup-object', tenantRef: tenantA })
    await seedJob({
      name: 'webhooks',
      state: 'active',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: tenantC,
    })

    const response = await adminApp.inject({
      method: 'GET',
      url: `/queue/overflow?limit=1&tenantRefs=${tenantA},${tenantB},${tenantC}`,
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      sourceTableExists: true,
      source: 'job',
      groupBy: 'summary',
      groupCount: 2,
      hasMore: true,
      totalCount: 3,
      filters: { tenantRefs: [tenantA, tenantB, tenantC] },
      data: [
        {
          count: 2,
          eventType: 'ObjectRemoved:Delete',
          name: 'webhooks',
        },
      ],
    })
  })

  it('returns the global created backlog without grouped list metadata', async () => {
    const baselineResponse = await adminApp.inject({
      method: 'GET',
      url: '/queue/overflow/count',
      headers,
    })
    expect(baselineResponse.statusCode).toBe(200)
    const baselineCount = baselineResponse.json().totalCount
    expect(Number.isInteger(baselineCount)).toBe(true)

    await seedJob({ name: 'webhooks', eventType: 'ObjectRemoved:Delete' })
    await seedJob({ name: 'backup-object' })
    await seedJob({ name: 'webhooks', state: 'active' })

    const response = await adminApp.inject({
      method: 'GET',
      url: '/queue/overflow/count',
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({ totalCount: baselineCount + 2 })
  })

  it('returns tenant hot spots for a filtered queue and event type', async () => {
    const tenantA = `${testRunId}-tenant-a`
    const tenantB = `${testRunId}-tenant-b`
    const tenantZ = `${testRunId}-tenant-z`
    await seedJob({ name: 'webhooks', eventType: 'ObjectRemoved:Delete', tenantRef: tenantB })
    await seedJob({ name: 'webhooks', eventType: 'ObjectRemoved:Delete', tenantRef: tenantB })
    await seedJob({ name: 'webhooks', eventType: 'ObjectRemoved:Delete', tenantRef: tenantA })
    await seedJob({
      name: 'backup-object',
      eventType: 'ObjectRemoved:Delete',
      tenantRef: tenantZ,
    })

    const response = await adminApp.inject({
      method: 'GET',
      url: `/queue/overflow?groupBy=tenant&name=webhooks&eventTypes=ObjectRemoved:Delete&tenantRefs=${tenantA},${tenantB},${tenantZ}`,
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      sourceTableExists: true,
      source: 'job',
      groupBy: 'tenant',
      groupCount: 2,
      hasMore: false,
      totalCount: 3,
      filters: {
        name: 'webhooks',
        eventTypes: ['ObjectRemoved:Delete'],
        tenantRefs: [tenantA, tenantB, tenantZ],
      },
      data: [
        { count: 2, tenantRef: tenantB },
        { count: 1, tenantRef: tenantA },
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
      sourceTableExists: false,
      source: 'backup',
      groupBy: 'summary',
      groupCount: 0,
      hasMore: false,
      totalCount: 0,
      filters: {},
      data: [],
    })
  })

  it('rejects accidental unfiltered and whitespace-only backups', async () => {
    const unfiltered = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/backup',
      headers,
      payload: {},
    })
    const whitespace = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/backup',
      headers,
      payload: { name: '   ' },
    })

    expect(unfiltered.statusCode).toBe(400)
    expect(whitespace.statusCode).toBe(400)
  })

  it('backs up only matching created tuple keys', async () => {
    const tenantRef = `${testRunId}-tenant-a`
    const sharedId = randomUUID()
    await seedJob({
      id: sharedId,
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef,
    })
    await seedJob({ id: sharedId, name: 'backup-object', tenantRef })
    const activeId = await seedJob({
      name: 'webhooks',
      state: 'active',
      eventType: 'ObjectRemoved:Delete',
      tenantRef,
    })

    const response = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/backup',
      headers,
      payload: {
        name: 'webhooks',
        eventTypes: ['ObjectRemoved:Delete'],
        tenantRefs: [tenantRef],
      },
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      backupTableCreated: true,
      filters: {
        name: 'webhooks',
        eventTypes: ['ObjectRemoved:Delete'],
        tenantRefs: [tenantRef],
      },
      limit: null,
      movedCount: 1,
    })
    await expect(findJob(pgBossJobTable, 'webhooks', sharedId)).resolves.toBeUndefined()
    await expect(findJob(pgBossBackupTable, 'webhooks', sharedId)).resolves.toEqual({
      id: sharedId,
      name: 'webhooks',
      state: 'created',
    })
    await expect(findJob(pgBossJobTable, 'backup-object', sharedId)).resolves.toBeDefined()
    await expect(findJob(pgBossJobTable, 'webhooks', activeId)).resolves.toBeDefined()
  })

  pgIt('rolls back the backup delete when restoring into the live table fails', async () => {
    const tenantRef = `${testRunId}-restore-failure`
    const id = await seedJob({
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef,
    })

    if (!queryDb) {
      throw new Error('Queue test database is not initialized')
    }

    const backupResponse = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/backup',
      headers,
      payload: { name: 'webhooks', tenantRefs: [tenantRef] },
    })

    expect(backupResponse.statusCode).toBe(200)
    await expect(findJob(pgBossJobTable, 'webhooks', id)).resolves.toBeUndefined()
    await expect(findJob(pgBossBackupTable, 'webhooks', id)).resolves.toBeDefined()

    await queryDb.query(`ALTER TABLE ${pgBossBackupTable} ALTER COLUMN priority DROP NOT NULL`)
    await queryDb.query({
      text: `UPDATE ${pgBossBackupTable} SET priority = NULL WHERE name = $1 AND id = $2`,
      values: ['webhooks', id],
    })

    const restoreResponse = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/restore',
      headers,
      payload: { name: 'webhooks', tenantRefs: [tenantRef] },
    })

    expect(restoreResponse.statusCode).toBe(500)
    await expect(findJob(pgBossBackupTable, 'webhooks', id)).resolves.toBeDefined()
    await expect(findJob(pgBossJobTable, 'webhooks', id)).resolves.toBeUndefined()
  })

  it('requires explicit confirmation before backing up all created jobs', async () => {
    await seedJob({ name: 'webhooks' })
    await seedJob({ name: 'backup-object' })
    await seedJob({ name: 'webhooks', state: 'active' })

    const response = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/backup',
      headers,
      payload: {
        confirmAll: true,
        limit: 1,
      },
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toMatchObject({ movedCount: 1, limit: 1 })
  })

  pgIt('restores backed up jobs in deterministic batches', async () => {
    const tenantRef = `${testRunId}-restore-batches`
    const firstId = await seedJob({
      id: '00000000-0000-0000-0000-000000000041',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef,
    })
    const secondId = await seedJob({
      id: '00000000-0000-0000-0000-000000000042',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef,
    })

    await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/backup',
      headers,
      payload: { name: 'webhooks', tenantRefs: [tenantRef] },
    })

    const batches: unknown[] = []
    let hasMore = true
    while (hasMore && batches.length < 10) {
      const response = await adminApp.inject({
        method: 'POST',
        url: '/queue/overflow/restore',
        headers,
        payload: { name: 'webhooks', limit: 1 },
      })

      expect(response.statusCode).toBe(200)
      const body = response.json()
      batches.push(body)
      hasMore = body.hasMore

      if (batches.length === 1) {
        await expect(findJob(pgBossJobTable, 'webhooks', firstId)).resolves.toBeDefined()
        await expect(findJob(pgBossJobTable, 'webhooks', secondId)).resolves.toBeUndefined()
      }
    }

    expect(hasMore).toBe(false)
    expect(batches).toEqual([
      {
        backupTableExists: true,
        conflictCount: 0,
        filters: { name: 'webhooks' },
        hasMore: true,
        limit: 1,
        movedCount: 1,
      },
      {
        backupTableExists: true,
        conflictCount: 0,
        filters: { name: 'webhooks' },
        hasMore: true,
        limit: 1,
        movedCount: 1,
      },
      {
        backupTableExists: true,
        conflictCount: 0,
        filters: { name: 'webhooks' },
        hasMore: false,
        limit: 1,
        movedCount: 0,
      },
    ])
    await expect(findJob(pgBossJobTable, 'webhooks', firstId)).resolves.toBeDefined()
    await expect(findJob(pgBossJobTable, 'webhooks', secondId)).resolves.toBeDefined()
    if (!queryDb) {
      throw new Error('Queue test database is not initialized')
    }
    await expect(
      queryDb.query<{ count: number }>({
        text: `SELECT COUNT(*)::int AS count FROM ${pgBossBackupTable}`,
      })
    ).resolves.toMatchObject({ rows: [{ count: 0 }] })
  })

  pgIt('drops archived conflicts while preserving the live job', async () => {
    const tenantRef = `${testRunId}-conflicts`
    const id = await seedJob({
      id: '00000000-0000-0000-0000-000000000001',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef,
    })
    const restorableId = await seedJob({
      id: '00000000-0000-0000-0000-000000000002',
      name: 'webhooks',
      eventType: 'ObjectRemoved:Delete',
      tenantRef,
    })
    await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/backup',
      headers,
      payload: { name: 'webhooks', tenantRefs: [tenantRef] },
    })
    await seedJob({ id, name: 'webhooks', eventType: 'ObjectCreated:Put' })

    const response = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/restore',
      headers,
      payload: { name: 'webhooks', limit: 10 },
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      backupTableExists: true,
      conflictCount: 1,
      filters: { name: 'webhooks' },
      hasMore: false,
      limit: 10,
      movedCount: 1,
    })
    await expect(findJob(pgBossBackupTable, 'webhooks', id)).resolves.toBeUndefined()
    await expect(findJob(pgBossJobTable, 'webhooks', id)).resolves.toBeDefined()
    await expect(findJobData(pgBossJobTable, 'webhooks', id)).resolves.toEqual({
      event: { type: 'ObjectCreated:Put' },
      queueOverflowTest: testRunId,
    })
    await expect(findJob(pgBossBackupTable, 'webhooks', restorableId)).resolves.toBeUndefined()
    await expect(findJob(pgBossJobTable, 'webhooks', restorableId)).resolves.toBeDefined()

    const followUp = await adminApp.inject({
      method: 'POST',
      url: '/queue/overflow/restore',
      headers,
      payload: { name: 'webhooks', limit: 10 },
    })

    expect(followUp.statusCode).toBe(200)
    expect(followUp.json()).toEqual({
      backupTableExists: true,
      conflictCount: 0,
      filters: { name: 'webhooks' },
      hasMore: false,
      limit: 10,
      movedCount: 0,
    })
  })

  pgIt('supports policy-job re-backup and restore', async () => {
    const schema = `qo_${randomUUID().replaceAll('-', '')}`
    const jobTable = `${schema}.job`
    const backupTable = `${schema}.job_overflow_backup`
    const id = randomUUID()
    const config = getConfig()
    const connectionString = resolveQueueTestConnectionString(config)

    if (!connectionString) {
      throw new Error('Expected a multitenant database URL for queue overflow integration test')
    }

    const queueDb = new QueueDB({
      connectionString,
      max: 1,
      statement_timeout: 5000,
    })
    const boss = new PgBoss({
      connectionString,
      db: queueDb,
      schema,
      schedule: false,
      supervise: false,
    })

    try {
      await boss.start()
      await boss.createQueue('webhooks', { name: 'webhooks', policy: 'standard' })
      await boss.createQueue('backup-object', { name: 'backup-object', policy: 'standard' })
      await boss.createQueue('exactly-once', { name: 'exactly-once', policy: 'exactly_once' })
      await boss.send('webhooks', { event: { type: 'ObjectRemoved:Delete' } }, { id })
      await boss.send('backup-object', {}, { id })

      if (!queryDb) {
        throw new Error('Queue test database is not initialized')
      }

      const store = new QueueOverflowStorePg(queueDb, schema)

      await queryDb.query(`CREATE TABLE ${backupTable} (LIKE ${jobTable} INCLUDING ALL)`)
      await expect(store.backup({ name: 'webhooks' })).resolves.toMatchObject({ movedCount: 1 })
      await expect(findJob(jobTable, 'webhooks', id, queryDb)).resolves.toBeUndefined()
      await expect(findJob(jobTable, 'backup-object', id, queryDb)).resolves.toBeDefined()
      await expect(findJob(backupTable, 'webhooks', id, queryDb)).resolves.toBeDefined()
      await expect(
        queryDb.query<{ count: number }>({
          text: `
            SELECT COUNT(*)::int AS count
            FROM pg_index
            WHERE indrelid = to_regclass($1)
              AND indisunique
              AND indpred IS NOT NULL
          `,
          values: [backupTable],
        })
      ).resolves.toMatchObject({ rows: [{ count: 0 }] })

      await expect(store.restore({ name: 'webhooks', limit: 1 })).resolves.toMatchObject({
        movedCount: 1,
      })
      await expect(findJob(jobTable, 'webhooks', id, queryDb)).resolves.toBeDefined()

      const archivedId = '00000000-0000-0000-0000-000000000011'
      const replacementId = '00000000-0000-0000-0000-000000000012'
      await boss.send('exactly-once', {}, { id: archivedId, singletonKey: 'tenant-a' })
      await expect(store.backup({ name: 'exactly-once' })).resolves.toMatchObject({
        movedCount: 1,
      })
      await queryDb.query({
        text: `
          INSERT INTO ${jobTable} (id, name, data, singleton_key, policy)
          VALUES ($1, $2, '{}'::jsonb, $3, $4)
        `,
        values: [replacementId, 'exactly-once', 'tenant-a', 'exactly_once'],
      })
      await expect(store.backup({ name: 'exactly-once' })).resolves.toMatchObject({
        movedCount: 1,
      })
      await expect(findJob(backupTable, 'exactly-once', archivedId, queryDb)).resolves.toBeDefined()
      await expect(
        findJob(backupTable, 'exactly-once', replacementId, queryDb)
      ).resolves.toBeDefined()

      await expect(store.restore({ name: 'exactly-once', limit: 10 })).resolves.toMatchObject({
        conflictCount: 1,
        hasMore: false,
        movedCount: 1,
      })
      await expect(
        findJob(backupTable, 'exactly-once', archivedId, queryDb)
      ).resolves.toBeUndefined()
      await expect(
        findJob(backupTable, 'exactly-once', replacementId, queryDb)
      ).resolves.toBeUndefined()
      await expect(findJob(jobTable, 'exactly-once', archivedId, queryDb)).resolves.toBeDefined()
      await expect(
        findJob(jobTable, 'exactly-once', replacementId, queryDb)
      ).resolves.toBeUndefined()
    } finally {
      await boss.stop({ graceful: false, wait: true }).catch(() => queueDb.close())
      if (queryDb) {
        await queryDb.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`)
      }
    }
  })
})
