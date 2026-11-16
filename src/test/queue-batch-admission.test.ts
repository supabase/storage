import { randomUUID } from 'node:crypto'
import { QueueDB } from '@internal/queue/database'
import { QueueBatchBusyError } from '@internal/queue/errors'
import { Client } from 'pg'
import PgBoss from 'pg-boss'
import { getConfig } from '../config'

describe('migration queue batch admission (PostgreSQL)', () => {
  const schema = `queue_admission_${randomUUID().replaceAll('-', '')}`
  const queueName = 'tenants-migrations-v2'
  const lockName = `${schema}.${queueName}`
  const applicationName = `${schema}_producer`
  let admin: Client
  let producer: QueueDB
  let contender: QueueDB
  let boss: PgBoss

  beforeAll(async () => {
    const connectionString = getConfig().databaseURL
    admin = new Client({ connectionString })
    await admin.connect()
    const setupDb = new QueueDB({
      connectionString,
      options: '-c multigres.unsafe_connection=on',
    })
    const setupBoss = new PgBoss({
      connectionString,
      schema,
      db: setupDb,
      schedule: false,
      supervise: false,
    })
    try {
      await setupBoss.start()
      await setupBoss.createQueue(queueName, { name: queueName, policy: 'exactly_once' })
    } finally {
      await setupBoss.stop({ graceful: false })
    }
    producer = new QueueDB({
      connectionString,
      application_name: applicationName,
      max: 1,
      statement_timeout: 1000,
    })
    contender = new QueueDB({ connectionString, max: 4, statement_timeout: 1000 })
    boss = new PgBoss({
      connectionString,
      schema,
      db: producer,
      migrate: false,
      schedule: false,
      supervise: false,
    })
    await boss.start()
    await contender.open()
  })

  afterAll(async () => {
    await boss?.stop({ graceful: false })
    await contender?.close()
    if (admin) {
      await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`)
      await admin.end()
    }
  })

  beforeEach(async () => {
    await admin.query(`DELETE FROM ${schema}.job`)
  })

  const job = (tenant: string): PgBoss.JobInsert => ({
    name: queueName,
    singletonKey: `migrations_${tenant}`,
    data: { tenantId: tenant },
    priority: 10,
    retryLimit: 3,
    retryDelay: 5,
  })

  async function jobCount() {
    const { rows } = await admin.query(`SELECT count(*)::int AS count FROM ${schema}.job`)
    return rows[0].count
  }

  async function waitForProducerQuery() {
    let pid: number | undefined
    await vi.waitFor(
      async () => {
        const { rows } = await admin.query<{ pid: number }>(
          "SELECT pid FROM pg_stat_activity WHERE application_name = $1 AND wait_event = 'PgSleep'",
          [applicationName]
        )
        pid = rows[0]?.pid
        expect(pid).toBeDefined()
      },
      { interval: 10, timeout: 1000 }
    )
    return pid!
  }

  it('rejects a competing producer before inserting, then admits it after the owner commits', async () => {
    const owner = producer.serializedBatchDb(lockName).executeSql('SELECT pg_sleep(0.5)', [])
    await waitForProducerQuery()
    try {
      await expect(
        boss.insert([job('a')], { db: contender.serializedBatchDb(lockName) })
      ).rejects.toBeInstanceOf(QueueBatchBusyError)
      expect(await jobCount()).toBe(0)
    } finally {
      await owner
    }

    await boss.insert([job('a')], { db: contender.serializedBatchDb(lockName) })
    expect(await jobCount()).toBe(1)
  })

  it.each(['statement timeout', 'connection loss'])(
    'releases admission after %s',
    async (failure) => {
      const owner = producer.serializedBatchDb(lockName).executeSql('SELECT pg_sleep(10)', [])
      // Attach rejection handling before terminating the backend or waiting for cancellation.
      const failed = expect(owner).rejects.toThrow()
      const pid = await waitForProducerQuery()
      if (failure === 'connection loss') {
        await admin.query('SELECT pg_terminate_backend($1)', [pid])
      }
      await failed

      await boss.insert([job('recovered')], { db: contender.serializedBatchDb(lockName) })
      expect(await jobCount()).toBe(1)
    }
  )
})
