import type { DatabaseStatement, DatabaseTransaction } from '@internal/database'
import {
  buildQueueOverflowWhereClause,
  normalizeQueueOverflowFilters,
  QUEUE_OVERFLOW_UNSCOPED_BACKUP_MESSAGE,
  type QueueOverflowDatabase,
  QueueOverflowStorePg,
} from './overflow'

function statementText(statement: string | DatabaseStatement): string {
  return typeof statement === 'string' ? statement : statement.text
}

function statementValues(statement: string | DatabaseStatement): unknown[] | undefined {
  return typeof statement === 'string' ? undefined : statement.values
}

function createMockDatabase(
  queryImplementation: (statement: string | DatabaseStatement) => Promise<{ rows: unknown[] }>
) {
  const query = vi.fn(queryImplementation)
  const commit = vi.fn().mockResolvedValue(undefined)
  const rollback = vi.fn().mockResolvedValue(undefined)
  const transaction = { query, commit, rollback } as unknown as DatabaseTransaction
  const beginTransaction = vi.fn().mockResolvedValue(transaction)
  const db: QueueOverflowDatabase = { beginTransaction }

  return { beginTransaction, commit, db, query, rollback }
}

describe('normalizeQueueOverflowFilters', () => {
  it('trims values and removes empty strings', () => {
    expect(
      normalizeQueueOverflowFilters({
        name: ' webhooks ',
        eventTypes: [' ObjectRemoved:Delete ', ''],
        tenantRefs: [' tenant-a ', 'tenant-a', '   '],
      })
    ).toEqual({
      name: 'webhooks',
      eventTypes: ['ObjectRemoved:Delete'],
      tenantRefs: ['tenant-a'],
    })
  })
})

describe('buildQueueOverflowWhereClause', () => {
  it('always scopes queries to created jobs', () => {
    expect(buildQueueOverflowWhereClause({})).toEqual({
      sql: 'state = $1',
      values: ['created'],
    })
  })

  it('uses native pg parameters and array bindings in a stable order', () => {
    expect(
      buildQueueOverflowWhereClause({
        name: ' webhooks ',
        eventTypes: ['ObjectRemoved:Delete', ' ObjectCreated:Put '],
        tenantRefs: ['tenant-b', 'tenant-a'],
      })
    ).toEqual({
      sql: "state = $1 AND name = $2 AND data->'event'->>'type' = ANY($3::text[]) AND data->'tenant'->>'ref' = ANY($4::text[])",
      values: [
        'created',
        'webhooks',
        ['ObjectRemoved:Delete', 'ObjectCreated:Put'],
        ['tenant-b', 'tenant-a'],
      ],
    })
  })
})

describe('QueueOverflowStorePg', () => {
  it('counts created jobs without grouping or window functions', async () => {
    const database = createMockDatabase(async (statement) => {
      const text = statementText(statement)

      if (text.includes("set_config('statement_timeout'")) {
        return { rows: [] }
      }

      return { rows: [{ total_count: '17' }] }
    })
    const store = new QueueOverflowStorePg(database.db)

    await expect(store.countCreated()).resolves.toEqual({ totalCount: 17 })

    const countCall = database.query.mock.calls.find(([statement]) =>
      statementText(statement).includes('COUNT(*)::bigint AS total_count')
    )
    expect(countCall).toBeDefined()
    expect(statementText(countCall?.[0] as string | DatabaseStatement)).not.toContain('GROUP BY')
    expect(statementText(countCall?.[0] as string | DatabaseStatement)).not.toContain('OVER (')
    expect(statementValues(countCall?.[0] as string | DatabaseStatement)).toEqual(['created'])
  })

  it('passes the abort signal while acquiring the maintenance transaction', async () => {
    const controller = new AbortController()
    const database = createMockDatabase(async (statement) => {
      const text = statementText(statement)

      if (text.includes('COUNT(*)::bigint AS total_count')) {
        return { rows: [{ total_count: '0' }] }
      }

      return { rows: [] }
    })
    const store = new QueueOverflowStorePg(database.db)

    await store.countCreated({ signal: controller.signal })

    expect(database.beginTransaction).toHaveBeenCalledWith({ signal: controller.signal })
  })

  it('reports exact totals and whether grouped list results were truncated', async () => {
    const database = createMockDatabase(async (statement) => {
      const text = statementText(statement)

      if (text.includes("set_config('statement_timeout'")) {
        return { rows: [] }
      }

      return {
        rows: [
          {
            count: '7',
            event_type: 'ObjectRemoved:Delete',
            group_count: '3',
            name: 'webhooks',
            total_count: '11',
          },
        ],
      }
    })
    const store = new QueueOverflowStorePg(database.db)

    await expect(store.list({ limit: 1 })).resolves.toEqual({
      sourceTableExists: true,
      data: [{ count: 7, eventType: 'ObjectRemoved:Delete', name: 'webhooks' }],
      filters: { eventTypes: undefined, name: undefined, tenantRefs: undefined },
      groupBy: 'summary',
      groupCount: 3,
      hasMore: true,
      source: 'job',
      totalCount: 11,
    })

    expect(database.commit).toHaveBeenCalledOnce()
    expect(database.rollback).not.toHaveBeenCalled()
    expect(statementValues(database.query.mock.calls[0][0])).toEqual(['0', '30s'])
  })

  it('rejects an unfiltered backup unless all jobs were explicitly confirmed', async () => {
    const database = createMockDatabase(async () => ({ rows: [] }))
    const store = new QueueOverflowStorePg(database.db)

    await expect(store.backup({})).rejects.toThrow(QUEUE_OVERFLOW_UNSCOPED_BACKUP_MESSAGE)
    await expect(store.backup({ name: '   ' })).rejects.toThrow(
      QUEUE_OVERFLOW_UNSCOPED_BACKUP_MESSAGE
    )
    expect(database.beginTransaction).not.toHaveBeenCalled()
  })

  it('moves matching tuple keys atomically', async () => {
    const database = createMockDatabase(async (statement) => {
      const text = statementText(statement)

      if (text.includes('SELECT to_regclass')) {
        return { rows: [{ table_name: null }] }
      }

      if (text.includes('WITH selected AS')) {
        return { rows: [{ moved_count: '2' }] }
      }

      return { rows: [] }
    })
    const store = new QueueOverflowStorePg(database.db)

    await expect(
      store.backup({
        name: 'webhooks',
        eventTypes: ['ObjectRemoved:Delete'],
        limit: 2,
      })
    ).resolves.toEqual({
      backupTableCreated: true,
      filters: {
        eventTypes: ['ObjectRemoved:Delete'],
        name: 'webhooks',
        tenantRefs: undefined,
      },
      limit: 2,
      movedCount: 2,
    })

    const moveCall = database.query.mock.calls.find(([statement]) =>
      statementText(statement).includes('WITH selected AS')
    )
    if (!moveCall) {
      throw new Error('Expected queue overflow move query')
    }

    const moveStatement = moveCall[0]
    const moveSql = statementText(moveStatement)
    expect(moveSql).toContain('SELECT name, id')
    expect(moveSql).toContain('source_job.name = selected.name')
    expect(moveSql).toContain('source_job.id = selected.id')
    expect(moveSql).toContain('INSERT INTO "pgboss_v10"."job_overflow_backup"')
    expect(moveSql).toContain('SELECT * FROM moved')
    expect(statementValues(moveStatement)).toEqual([
      'created',
      'webhooks',
      ['ObjectRemoved:Delete'],
      2,
    ])

    const createTableCall = database.query.mock.calls.find(([statement]) =>
      statementText(statement).includes('CREATE TABLE IF NOT EXISTS')
    )
    const createTableSql = statementText(createTableCall?.[0] as string | DatabaseStatement)
    expect(createTableSql).toContain('LIKE "pgboss_v10"."job" INCLUDING DEFAULTS')
    expect(createTableSql).toContain('PRIMARY KEY (name, id)')
    expect(createTableSql).not.toContain('INCLUDING ALL')
  })

  it('removes partial unique indexes inherited by existing backup tables', async () => {
    const database = createMockDatabase(async (statement) => {
      const text = statementText(statement)

      if (text.includes('SELECT to_regclass')) {
        return { rows: [{ table_name: 'pgboss_v10.job_overflow_backup' }] }
      }

      if (text.includes('FROM pg_index AS backup_index')) {
        return {
          rows: [
            { index_name: 'pgboss_v10.job_overflow_backup_name_idx' },
            { index_name: 'pgboss_v10.job_overflow_backup_singleton_idx' },
          ],
        }
      }

      if (text.includes('WITH selected AS')) {
        return { rows: [{ moved_count: '1' }] }
      }

      return { rows: [] }
    })
    const store = new QueueOverflowStorePg(database.db)

    await expect(store.backup({ name: 'exactly-once' })).resolves.toMatchObject({
      backupTableCreated: false,
      movedCount: 1,
    })

    const dropCalls = database.query.mock.calls
      .map(([statement]) => statementText(statement))
      .filter((statement) => statement.startsWith('DROP INDEX IF EXISTS'))
    expect(dropCalls).toEqual([
      'DROP INDEX IF EXISTS pgboss_v10.job_overflow_backup_name_idx',
      'DROP INDEX IF EXISTS pgboss_v10.job_overflow_backup_singleton_idx',
    ])
  })

  it('commits a completed move if the signal aborts before finalization', async () => {
    const controller = new AbortController()
    let markMoveStarted: () => void = () => undefined
    let resolveMove: (result: { rows: unknown[] }) => void = () => undefined
    const moveStarted = new Promise<void>((resolve) => {
      markMoveStarted = resolve
    })
    const moveResult = new Promise<{ rows: unknown[] }>((resolve) => {
      resolveMove = resolve
    })
    const database = createMockDatabase((statement) => {
      const text = statementText(statement)

      if (text.includes('SELECT to_regclass')) {
        return Promise.resolve({ rows: [{ table_name: null }] })
      }

      if (text.includes('WITH selected AS')) {
        markMoveStarted()
        return moveResult
      }

      return Promise.resolve({ rows: [] })
    })
    const store = new QueueOverflowStorePg(database.db)
    const operation = store.backup({ name: 'webhooks', signal: controller.signal })

    await moveStarted
    resolveMove({ rows: [{ moved_count: '1' }] })
    controller.abort()

    await expect(operation).resolves.toMatchObject({ movedCount: 1 })
    expect(database.commit).toHaveBeenCalledOnce()
    expect(database.rollback).not.toHaveBeenCalled()
  })

  it('restores non-conflicting rows and reports conflicts dropped from the backup', async () => {
    const database = createMockDatabase(async (statement) => {
      const text = statementText(statement)

      if (text.includes('SELECT to_regclass')) {
        return { rows: [{ table_name: 'pgboss_v10.job_overflow_backup' }] }
      }

      if (text.includes('WITH selected AS')) {
        return {
          rows: [
            {
              selected_count: '3',
              moved_count: '2',
            },
          ],
        }
      }

      return { rows: [] }
    })
    const store = new QueueOverflowStorePg(database.db)

    await expect(store.restore({ name: 'webhooks', limit: 3 })).resolves.toEqual({
      backupTableExists: true,
      conflictCount: 1,
      filters: { eventTypes: undefined, name: 'webhooks', tenantRefs: undefined },
      hasMore: true,
      limit: 3,
      movedCount: 2,
    })

    const restoreCall = database.query.mock.calls.find(([statement]) =>
      statementText(statement).includes('WITH selected AS')
    )
    expect(restoreCall).toBeDefined()
    const restoreSql = statementText(restoreCall?.[0] as string | DatabaseStatement)
    expect(restoreSql).toContain('ON CONFLICT DO NOTHING')
    expect(restoreSql).toContain('SELECT * FROM selected')
    expect(restoreSql).toContain('DELETE FROM "pgboss_v10"."job_overflow_backup"')
    expect(restoreSql).toContain('USING selected')
    expect(restoreSql).not.toContain('live_conflicts AS')
    expect(statementValues(restoreCall?.[0] as string | DatabaseStatement)).toEqual([
      'created',
      'webhooks',
      3,
    ])

    expect(
      database.query.mock.calls.some(([statement]) =>
        statementText(statement).includes(
          'LOCK TABLE "pgboss_v10"."job" IN SHARE ROW EXCLUSIVE MODE'
        )
      )
    ).toBe(true)
  })

  it('rejects restore on the effective OrioleDB queue connection', async () => {
    const database = createMockDatabase(async (statement) => {
      const text = statementText(statement)

      if (text.includes('SELECT to_regclass')) {
        return { rows: [{ table_name: 'pgboss_v10.job_overflow_backup' }] }
      }

      if (text.includes("extname = 'orioledb'")) {
        return { rows: [{ is_oriole: true }] }
      }

      return { rows: [] }
    })
    const store = new QueueOverflowStorePg(database.db)

    await expect(store.restore({ limit: 1 })).rejects.toMatchObject({
      code: 'NotSupported',
      message: 'Queue overflow restore is not supported on OrioleDB',
    })
    expect(
      database.query.mock.calls.some(([statement]) =>
        statementText(statement).includes('WITH selected AS')
      )
    ).toBe(false)
    expect(
      database.query.mock.calls.some(([statement]) =>
        statementText(statement).includes('LOCK TABLE')
      )
    ).toBe(false)
  })

  it('returns the new default limit and no more work when the backup table is missing', async () => {
    const database = createMockDatabase(async (statement) => {
      const text = statementText(statement)

      if (text.includes('SELECT to_regclass')) {
        return { rows: [{ table_name: null }] }
      }

      return { rows: [] }
    })
    const store = new QueueOverflowStorePg(database.db)

    await expect(store.restore({})).resolves.toEqual({
      backupTableExists: false,
      conflictCount: 0,
      filters: { eventTypes: undefined, name: undefined, tenantRefs: undefined },
      hasMore: false,
      limit: 10000,
      movedCount: 0,
    })
  })

  it('rolls back and preserves the primary move failure', async () => {
    const moveError = new Error('move failed')
    const database = createMockDatabase(async (statement) => {
      const text = statementText(statement)

      if (text.includes('SELECT to_regclass')) {
        return { rows: [{ table_name: 'pgboss_v10.job_overflow_backup' }] }
      }

      if (text.includes('WITH selected AS')) {
        throw moveError
      }

      return { rows: [] }
    })
    const store = new QueueOverflowStorePg(database.db)

    await expect(store.restore({ limit: 1 })).rejects.toBe(moveError)
    expect(database.rollback).toHaveBeenCalledOnce()
    expect(database.commit).not.toHaveBeenCalled()
  })
})
