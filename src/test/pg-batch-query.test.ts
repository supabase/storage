import { BatchQuery } from '@internal/database/postgres/batch-query'
import { Client } from 'pg'
import { getConfig } from '../config'

const { databaseURL } = getConfig()
const table = 'pg_batch_query_check'

describe.each([
  ['a pipelining client', true],
  ['a regular client', false],
])('BatchQuery on %s', (_, pipeline) => {
  let client: Client

  function run(
    statements: Array<{ text: string; values?: unknown[] }>,
    begin?: string,
    end?: string
  ) {
    const batch = new BatchQuery(statements, begin, end)
    client.query(batch)
    return batch
  }

  async function count(): Promise<number> {
    const { rows } = await client.query(`SELECT count(*)::int AS n FROM ${table}`)
    return rows[0].n
  }

  beforeAll(async () => {
    client = new Client({ connectionString: databaseURL, pipeline })
    await client.connect()
    await client.query(`CREATE TABLE IF NOT EXISTS ${table} (n int)`)
  })

  beforeEach(async () => {
    await client.query(`TRUNCATE ${table}`)
  })

  afterAll(async () => {
    await client.query(`DROP TABLE IF EXISTS ${table}`)
    await client.end()
  })

  it('parses rows as a regular query does, each statement its own result', async () => {
    const sql = `SELECT 1::int AS n, '{"a":1}'::jsonb AS j, true AS b, NULL::text AS z,
                 '2026-11-26T10:00:00Z'::timestamptz AS t, $1::text AS v`
    const batch = run(
      [{ text: sql, values: ['x'] }, { text: `INSERT INTO ${table} VALUES (1), (2)` }],
      'BEGIN',
      'COMMIT'
    )

    const [selected, inserted] = await batch.promise
    const regular = await client.query(sql, ['x'])

    expect(selected.rows).toEqual(regular.rows)
    expect(selected.fields.map((field) => field.name)).toEqual(
      regular.fields.map((field) => field.name)
    )
    expect(selected.rowCount).toBe(1)
    expect(inserted.command).toBe('INSERT')
    expect(inserted.rowCount).toBe(2)
    expect(await count()).toBe(2)
  })

  it('keeps queries queued around it in order', async () => {
    const before = client.query('SELECT 1 AS n')
    const batch = run([{ text: 'SELECT 2 AS n' }], 'BEGIN', 'COMMIT')
    const after = client.query('SELECT 3 AS n')

    expect((await before).rows).toEqual([{ n: 1 }])
    expect((await batch.promise)[0].rows).toEqual([{ n: 2 }])
    expect((await after).rows).toEqual([{ n: 3 }])
  })

  it('is not submitted when the client cannot send it', async () => {
    const closed = new Client({ connectionString: databaseURL, pipeline })
    await closed.connect()
    await closed.end()

    const batch = new BatchQuery([{ text: 'SELECT 1' }], 'BEGIN', 'COMMIT')
    closed.query(batch)

    await expect(batch.promise).rejects.toThrow('not queryable')
    expect(batch.submitted).toBe(false)
  })

  it('runs nothing behind a BEGIN that fails', async () => {
    const batch = run(
      [
        { text: "SELECT set_config('request.jwt.claim.sub', 'someone', true)" },
        { text: `INSERT INTO ${table} VALUES (1)` },
      ],
      'BEGIN ISOLATION LEVEL NONE',
      'COMMIT'
    )

    await expect(batch.promise).rejects.toMatchObject({ code: '42601' })
    expect(batch.beganTransaction).toBe(false)
    expect(batch.submitted).toBe(true)
    expect(await count()).toBe(0)
  })

  it('skips the rest after a failure, COMMIT included, leaving the transaction aborted', async () => {
    const batch = run(
      [
        { text: `INSERT INTO ${table} VALUES (1)` },
        { text: 'SELECT 1/0' },
        { text: `INSERT INTO ${table} VALUES (2)` },
      ],
      'BEGIN',
      'COMMIT'
    )

    await expect(batch.promise).rejects.toMatchObject({ code: '22012' })
    expect(batch.beganTransaction).toBe(true)
    expect(batch.reachedEnd).toBe(false)
    await expect(client.query('SELECT 1')).rejects.toMatchObject({ code: '25P02' })
    await client.query('ROLLBACK')
    expect(await count()).toBe(0)
  })

  it('reports a failed COMMIT as the end of the batch', async () => {
    await client.query(
      `CREATE TEMP TABLE IF NOT EXISTS pg_batch_query_deferred (n int UNIQUE DEFERRABLE INITIALLY DEFERRED)`
    )
    const batch = run(
      [{ text: 'INSERT INTO pg_batch_query_deferred VALUES (1), (1)' }],
      'BEGIN',
      'COMMIT'
    )

    await expect(batch.promise).rejects.toMatchObject({ code: '23505' })
    expect(batch.reachedEnd).toBe(true)
    expect((await client.query('SELECT 1 AS n')).rows).toEqual([{ n: 1 }])
  })
})
