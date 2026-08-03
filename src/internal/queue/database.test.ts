import { createRequire } from 'node:module'
import type { DatabaseExecutor, DatabaseStatement } from '@internal/database'
import { TransactionalQueueDb, txQueueCtx } from './database'

// pg-boss's plans module is internal (not on the package's export surface), so it loads via a
// direct CJS require of the dist file.
const loadCjs = createRequire(__filename)

type PgBossPlans = {
  insertJobs(schema: string, options: { table: string; name: string }): string
}

describe('TransactionalQueueDb', () => {
  it('passes pg-boss SQL and positional values through without placeholder rewriting', async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [{ ok: true }],
    })
    const db = new TransactionalQueueDb({ query } as unknown as DatabaseExecutor)

    await expect(db.executeSql('SELECT $1, $2, $3', ['queue-name', undefined, 3])).resolves.toEqual(
      {
        rows: [{ ok: true }],
      }
    )

    expect(query).toHaveBeenCalledWith({
      text: 'SELECT $1, $2, $3',
      values: ['queue-name', null, 3],
    } satisfies DatabaseStatement)
  })

  it('txQueueCtx wraps the executor in a TransactionalQueueDb', () => {
    const executor = { query: vi.fn() } as unknown as DatabaseExecutor

    expect(txQueueCtx(executor)).toBeInstanceOf(TransactionalQueueDb)
  })

  // The transactional produce ctx only ever executes pg-boss's insert path (wave's pgboss
  // adapter routes `append` with a ctx through `boss.insert(queue, jobs, { db })`), and
  // `executeSql` passes that SQL to pg verbatim — so the insert plan must use only
  // pg-compatible `$n` placeholders (no `?` or `:name` rewriting is ever performed).
  it('covers the pg-boss v12 insert path with only pg-compatible placeholders', () => {
    const plans = loadCjs('pg-boss/dist/plans') as PgBossPlans

    const sql = plans.insertJobs('pgboss_v12', { table: 'job', name: 'test-queue' })
    const stripped = sql
      .replace(/\$[A-Za-z_]*\$[\s\S]*?\$[A-Za-z_]*\$/g, '')
      .replace(/'(?:''|[^'])*'/g, '')

    expect(stripped).not.toContain('?')
    expect(stripped.match(/(^|[^\w:]):[A-Za-z_][A-Za-z0-9_]*/g) || []).toEqual([])
  })
})
