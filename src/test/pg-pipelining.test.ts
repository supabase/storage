import { randomInt } from 'node:crypto'
import { PgPoolExecutor } from '@internal/database/pg-connection'
import * as cancellation from '@internal/database/postgres/cancellation'
import { Client, Pool, type PoolClient, type QueryResult } from 'pg'
import { getConfig } from '../config'

describe('pg pipelining with failed cancellation', () => {
  const modes = ['standalone', 'transaction'] as const

  it.each(modes)('unblocks queued work after %s abort', async (mode) => {
    const { databaseURL } = getConfig()
    const pool = new Pool({
      connectionString: databaseURL,
      max: 1,
      connectionTimeoutMillis: 1000,
      pipeline: true,
    })
    const blocker = new Client({ connectionString: databaseURL })
    const controller = new AbortController()
    const lockId = randomInt(1, 2_147_483_647)
    const executor = new PgPoolExecutor(pool)
    const acquiredClients: PoolClient[] = []
    pool.on('acquire', (client) => acquiredClients.push(client))
    let query: Promise<unknown> | undefined
    let queued: Promise<QueryResult<{ n: number }>> | undefined

    // Model a proxy that cannot deliver PostgreSQL's best-effort cancel request.
    const cancel = vi.spyOn(cancellation, 'cancelQuery').mockResolvedValue(undefined)

    await blocker.connect()
    await blocker.query('SELECT pg_advisory_lock($1)', [lockId])

    try {
      const transaction = mode === 'transaction' ? await executor.beginTransaction() : undefined
      query = (transaction ?? executor)
        .query(
          { text: 'SELECT pg_advisory_xact_lock($1)', values: [lockId] },
          { signal: controller.signal }
        )
        .catch((error: unknown) => error)

      // Multigres can change physical backends between statements, so identify
      // the blocked query by its lock key rather than a previously observed PID.
      await vi.waitFor(
        async () => {
          const locks = await blocker.query<{ waiting: boolean }>(
            `SELECT EXISTS (
              SELECT 1 FROM pg_locks
              WHERE locktype = 'advisory' AND classid = 0 AND objid = $1
                AND objsubid = 1 AND NOT granted
            ) AS waiting`,
            [lockId]
          )
          expect(locks.rows[0].waiting).toBe(true)
        },
        { timeout: 5000 }
      )

      expect(acquiredClients).toHaveLength(1)
      const [abortedClient] = acquiredClients
      queued = executor.query<{ n: number }>('SELECT 1 AS n')
      expect(pool.waitingCount).toBe(1)
      controller.abort()

      await expect(query).resolves.toMatchObject({ name: 'AbortError', code: 'ABORT_ERR' })
      expect(cancel).toHaveBeenCalledWith(abortedClient)
      await expect(queued).resolves.toMatchObject({ rows: [{ n: 1 }] })
      expect(acquiredClients).toHaveLength(2)
      const replacementClient = acquiredClients[1]
      expect(replacementClient).not.toBe(abortedClient)

      await expect(executor.query('SELECT 1 / 0')).rejects.toMatchObject({ code: '22012' })
      await expect(executor.query('SELECT 2 AS n')).resolves.toMatchObject({ rows: [{ n: 2 }] })
      expect(acquiredClients).toHaveLength(4)
      expect(acquiredClients[2]).toBe(replacementClient)
      expect(acquiredClients[3]).toBe(replacementClient)

      if (transaction) {
        expect(transaction.isCompleted()).toBe(true)
        await transaction.rollback()
      }
    } finally {
      controller.abort()
      await blocker.query('SELECT pg_advisory_unlock($1)', [lockId])
      await Promise.allSettled([query, queued])
      await pool.end()
      await blocker.end()
    }
  })
})
