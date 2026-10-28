import { randomUUID } from 'node:crypto'
import { PG_BOSS_SCHEMA, Queue } from '@internal/queue'
import { QueueDB, queueTimestampText } from '@internal/queue/database'
import PgBoss from 'pg-boss'
import { getConfig } from '../config'

class TestQueue extends Queue {
  static release(queueName: string, jobs: PgBoss.JobWithMetadata<unknown>[]) {
    return this.releaseJobs(queueName, jobs)
  }
}

const { pgQueueConnectionURL, databaseURL } = getConfig()
const db = new QueueDB({
  connectionString: pgQueueConnectionURL || databaseURL,
  max: 2,
  options: '-c multigres.unsafe_connection=on',
})
const boss = new PgBoss({
  db,
  schema: PG_BOSS_SCHEMA,
  supervise: false,
  schedule: false,
})
const queues: string[] = []

async function createQueue(policy: PgBoss.Queue['policy'] = 'standard') {
  const name = `shutdown-${randomUUID()}`
  const deadLetter = `${name}-dlq`
  await boss.createQueue(deadLetter)
  queues.push(deadLetter)
  await boss.createQueue(name, { name, policy, deadLetter })
  queues.push(name)
  return { name, deadLetter }
}

beforeAll(async () => {
  await boss.start()
})

beforeEach(() => {
  vi.spyOn(Queue, 'getDb').mockReturnValue(db)
})

afterAll(async () => {
  try {
    for (const name of queues.reverse()) {
      await db.executeSql(`DELETE FROM ${PG_BOSS_SCHEMA}.job WHERE name = $1`, [name])
      await boss.deleteQueue(name)
    }
  } finally {
    await boss.stop({ graceful: false })
  }
})

describe('Queue shutdown job release', () => {
  it.each(['standard', 'singleton', 'exactly_once'] as const)(
    'returns an unstarted %s job without exhausting its zero retry limit',
    async (policy) => {
      const { name, deadLetter } = await createQueue(policy)
      const id = (await boss.send(name, {}, { retryLimit: 0 }))!
      const [fetched] = await boss.fetch(name, { includeMetadata: true, db: db.jobFetchDb })
      expect(fetched).toMatchObject({ id, retryCount: 0, retryLimit: 0 })
      const ordinary = (await boss.getJobById(name, id))!
      expect(queueTimestampText(fetched.startedOn)).toBeDefined()
      expect(queueTimestampText(ordinary.startedOn)).toBeUndefined()
      expect(fetched.startedOn.getTime()).toBe(ordinary.startedOn.getTime())

      await TestQueue.release(name, [fetched])
      await TestQueue.release(name, [fetched])
      expect(await boss.getJobById(name, id)).toMatchObject({
        state: 'retry',
        retryCount: 0,
        retryLimit: 0,
        startedOn: null,
      })
      expect(await boss.fetch(deadLetter)).toEqual([])

      const [next] = await boss.fetch(name, { includeMetadata: true, db: db.jobFetchDb })
      expect(next).toMatchObject({ id, retryCount: 0, retryLimit: 0 })
      await boss.complete(name, id)
    }
  )

  it('preserves the final retry and still fails a job whose handler uses that attempt', async () => {
    const { name, deadLetter } = await createQueue()
    const id = (await boss.send(name, {}, { retryLimit: 1, retryDelay: 0 }))!
    await boss.fetch(name)
    await boss.fail(name, id)
    const [lastAttempt] = await boss.fetch(name, { includeMetadata: true, db: db.jobFetchDb })
    expect(lastAttempt).toMatchObject({ id, retryCount: 1, retryLimit: 1 })

    await TestQueue.release(name, [lastAttempt])
    const [next] = await boss.fetch(name, { includeMetadata: true, db: db.jobFetchDb })
    expect(next).toMatchObject({ id, retryCount: 1, retryLimit: 1 })
    expect(await boss.fetch(deadLetter)).toEqual([])

    await boss.fail(name, id)
    expect(await boss.getJobById(name, id)).toMatchObject({ state: 'failed' })
    expect(await boss.fetch(deadLetter)).toHaveLength(1)
  })

  it('leaves completed jobs and jobs from another queue unchanged', async () => {
    const { name } = await createQueue()
    const { name: other } = await createQueue()
    const id = (await boss.send(name, {}))!
    const [fetched] = await boss.fetch(name, { includeMetadata: true, db: db.jobFetchDb })

    await TestQueue.release(other, [fetched])
    expect(await boss.getJobById(name, id)).toMatchObject({ state: 'active' })

    await boss.complete(name, id)
    await TestQueue.release(name, [fetched])
    expect(await boss.getJobById(name, id)).toMatchObject({ state: 'completed' })
  })
  it('does not release a newer active attempt fetched by another worker', async () => {
    const { name } = await createQueue('exactly_once')
    const id = (await boss.send(name, {}, { retryLimit: 2, retryDelay: 0 }))!
    const [oldAttempt] = await boss.fetch(name, { includeMetadata: true, db: db.jobFetchDb })
    await boss.fail(name, id)
    const [newAttempt] = await boss.fetch(name, { includeMetadata: true, db: db.jobFetchDb })

    await TestQueue.release(name, [oldAttempt])
    expect(await boss.getJobById(name, id)).toMatchObject({ state: 'active', retryCount: 1 })
    expect(await boss.fetch(name)).toEqual([])
    await TestQueue.release(name, [newAttempt])
    expect(await boss.getJobById(name, id)).toMatchObject({ state: 'retry', retryCount: 1 })
  })

  it('compares PostgreSQL timestamp precision beyond milliseconds', async () => {
    const { name } = await createQueue()
    const id = (await boss.send(name, {}))!
    await boss.fetch(name, { includeMetadata: true, db: db.jobFetchDb })
    await db.executeSql(
      `UPDATE ${PG_BOSS_SCHEMA}.job SET started_on = '2026-01-01 00:00:00.123456+00' WHERE name = $1 AND id = $2`,
      [name, id]
    )
    const oldAttempt = (await boss.getJobById(name, id, {
      db: db.jobFetchDb,
      includeArchive: false,
    }))!
    await db.executeSql(
      `UPDATE ${PG_BOSS_SCHEMA}.job SET started_on = '2026-01-01 00:00:00.123789+00' WHERE name = $1 AND id = $2`,
      [name, id]
    )
    const newAttempt = (await boss.getJobById(name, id, {
      db: db.jobFetchDb,
      includeArchive: false,
    }))!
    expect(oldAttempt.startedOn.getTime()).toBe(newAttempt.startedOn.getTime())

    await TestQueue.release(name, [oldAttempt])
    expect(await boss.getJobById(name, id)).toMatchObject({ state: 'active' })

    // Even if the database clock repeats a timestamp, the retry count fences the attempt.
    await db.executeSql(
      `UPDATE ${PG_BOSS_SCHEMA}.job SET started_on = '2026-01-01 00:00:00.123456+00', retry_count = 1 WHERE name = $1 AND id = $2`,
      [name, id]
    )
    await TestQueue.release(name, [oldAttempt])
    const currentAttempt = (await boss.getJobById(name, id, {
      db: db.jobFetchDb,
      includeArchive: false,
    }))!
    expect(currentAttempt).toMatchObject({ state: 'active', retryCount: 1 })
    await TestQueue.release(name, [currentAttempt])
    expect(await boss.getJobById(name, id)).toMatchObject({ state: 'retry' })
  })
})
