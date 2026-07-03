import { createRequire } from 'node:module'
import type { PgExecutor, PgStatement } from '@internal/database'
import { PgQueueDB } from './database'

const loadCjs = createRequire(__filename)

type PgBossStatic = {
  getConstructionPlans(schema: string): string
  getMigrationPlans(schema: string, version: number): string
  getRollbackPlans(schema: string, version: number): string
}

type SqlQuery = {
  text: string
  values: unknown[]
}

type FetchJobOptions = {
  schema: string
  table: string
  name: string
  policy: string | undefined
  limit: number
  includeMetadata?: boolean
  priority?: boolean
  orderByCreatedOn?: boolean
  ignoreStartAfter?: boolean
  ignoreSingletons: string[] | null
}

type PgBossMigration = {
  version: number
  previous: number
}

type PgBossPlans = {
  JOB_STATES: Record<string, string>
  create(schema: string, version: number, options?: { createSchema?: boolean }): string
  insertVersion(schema: string, version: number): string
  getVersion(schema: string): string
  setVersion(schema: string, version: number): string
  fetchNextJob(options: FetchJobOptions): SqlQuery
  completeJobs(schema: string, table: string, includeQueued?: boolean): string
  cancelJobs(schema: string, table: string): string
  resumeJobs(schema: string, table: string): string
  restoreJobs(schema: string, table: string): string
  retryJobs(schema: string, table: string): string
  deleteJobsById(schema: string, table: string): string
  deleteQueuedJobs(schema: string, table: string): string
  deleteStoredJobs(schema: string, table: string): string
  deleteAllJobs(schema: string, table: string): string
  truncateTable(schema: string, table: string): string
  failJobsById(schema: string, table: string): string
  failJobsByTimeout(schema: string, table: string, queues: string[]): string
  failJobsByHeartbeat(schema: string, table: string, queues: string[]): string
  touchJobs(schema: string, table: string): string
  insertJobs(schema: string, options: { table: string; name: string; returnId?: boolean }): string
  getTime(): string
  getSchedules(schema: string): string
  getSchedulesByQueue(schema: string): string
  schedule(schema: string): string
  unschedule(schema: string): string
  subscribe(schema: string): string
  unsubscribe(schema: string): string
  getQueuesForEvent(schema: string): string
  deletion(schema: string, table: string, queues: string[]): string
  getQueueStats(schema: string, table: string, queues: string[]): SqlQuery
  cacheQueueStats(schema: string, table: string, queues: string[]): string
  createQueue(schema: string, name: string, options: unknown): string
  updateQueue(schema: string, options?: { deadLetter?: string }): string
  deleteQueue(schema: string, name: string): string
  getQueues(schema: string, names?: string[]): SqlQuery
  getJobById(schema: string, table: string): string
  findJobs(
    schema: string,
    table: string,
    options: { queued: boolean; byKey: boolean; byData: boolean; byId: boolean }
  ): string
  getBlockedKeys(schema: string, table: string): string
  trySetQueueMonitorTime(schema: string, queues: string[], seconds: number): SqlQuery
  trySetQueueDeletionTime(schema: string, queues: string[], seconds: number): SqlQuery
  trySetCronTime(schema: string, seconds: number): string
  trySetBamTime(schema: string, seconds: number): string
  insertWarning(schema: string): string
  getWarnings(schema: string): string
  getWarningsCount(schema: string): string
  deleteOldWarnings(schema: string, days: number): string
  getBamStatus(schema: string): string
  getBamEntries(schema: string): string
  getNextBamCommand(schema: string): string
  setBamCompleted(schema: string, id: string): string
  setBamFailed(schema: string, id: string, error: string): string
  locked(schema: string, query: string | string[], key?: string): string
  assertMigration(schema: string, version: number): string
}

type MigrationStore = {
  getAll(schema: string): PgBossMigration[]
}

const PgBoss = loadCjs('pg-boss') as PgBossStatic
const plans = loadCjs('pg-boss/dist/plans') as PgBossPlans
const migrationStore = loadCjs('pg-boss/dist/migrationStore') as MigrationStore
const schemaVersion = (loadCjs('pg-boss/package.json') as { pgboss: { schema: number } }).pgboss
  .schema

function normalizeSql(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim()
}

function stripSqlStrings(sql: string): string {
  return (
    sql
      .replace(/\$[A-Za-z_]*\$[\s\S]*?\$[A-Za-z_]*\$/g, '')
      // Drop the jsonb key-exists operator (e.g. "o.data ? 'retryDelayMax'") so it
      // is not mistaken for a question-mark placeholder.
      .replace(/\?(?=\s*')/g, ' ')
      .replace(/'(?:''|[^'])*'/g, '')
  )
}

function collectPgBossSqlSurface(): Array<[string, string]> {
  const schema = 'pgboss_v12'
  const table = 'job_common'
  const queues = ['queue-name']
  const migrations = migrationStore.getAll(schema) as PgBossMigration[]
  const statements: Array<[string, string]> = [
    ['create', plans.create(schema, schemaVersion)],
    ['insertVersion', plans.insertVersion(schema, schemaVersion)],
    ['getVersion', plans.getVersion(schema)],
    ['setVersion', plans.setVersion(schema, schemaVersion)],
    [
      'fetchNextJob default',
      plans.fetchNextJob({
        schema,
        table,
        name: 'queue-name',
        policy: 'standard',
        limit: 5,
        ignoreSingletons: null,
      }).text,
    ],
    [
      'fetchNextJob metadata no-priority ignore-start-after',
      plans.fetchNextJob({
        schema,
        table,
        name: 'queue-name',
        policy: 'standard',
        limit: 5,
        includeMetadata: true,
        priority: false,
        ignoreStartAfter: true,
        ignoreSingletons: ['singleton-key'],
      }).text,
    ],
    ['completeJobs', plans.completeJobs(schema, table)],
    ['completeJobs queued', plans.completeJobs(schema, table, true)],
    ['cancelJobs', plans.cancelJobs(schema, table)],
    ['resumeJobs', plans.resumeJobs(schema, table)],
    ['restoreJobs', plans.restoreJobs(schema, table)],
    ['retryJobs', plans.retryJobs(schema, table)],
    ['deleteJobsById', plans.deleteJobsById(schema, table)],
    ['deleteQueuedJobs', plans.deleteQueuedJobs(schema, table)],
    ['deleteStoredJobs', plans.deleteStoredJobs(schema, table)],
    ['deleteAllJobs', plans.deleteAllJobs(schema, table)],
    ['truncateTable', plans.truncateTable(schema, table)],
    ['failJobsById', plans.failJobsById(schema, table)],
    ['failJobsByTimeout', plans.failJobsByTimeout(schema, table, queues)],
    ['failJobsByHeartbeat', plans.failJobsByHeartbeat(schema, table, queues)],
    ['touchJobs', plans.touchJobs(schema, table)],
    ['insertJobs', plans.insertJobs(schema, { table, name: 'queue-name' })],
    ['getTime', plans.getTime()],
    ['getSchedules', plans.getSchedules(schema)],
    ['getSchedulesByQueue', plans.getSchedulesByQueue(schema)],
    ['schedule', plans.schedule(schema)],
    ['unschedule', plans.unschedule(schema)],
    ['subscribe', plans.subscribe(schema)],
    ['unsubscribe', plans.unsubscribe(schema)],
    ['getQueuesForEvent', plans.getQueuesForEvent(schema)],
    ['deletion', plans.deletion(schema, table, queues)],
    ['getQueueStats', plans.getQueueStats(schema, table, queues).text],
    ['cacheQueueStats', plans.cacheQueueStats(schema, table, queues)],
    ['createQueue', plans.createQueue(schema, 'queue-name', { policy: 'standard' })],
    ['updateQueue', plans.updateQueue(schema, { deadLetter: 'dead-letter-queue' })],
    ['deleteQueue', plans.deleteQueue(schema, 'queue-name')],
    ['getQueues', plans.getQueues(schema, queues).text],
    ['getJobById', plans.getJobById(schema, table)],
    [
      'findJobs',
      plans.findJobs(schema, table, { queued: true, byKey: true, byData: true, byId: true }),
    ],
    ['getBlockedKeys', plans.getBlockedKeys(schema, table)],
    ['trySetQueueMonitorTime', plans.trySetQueueMonitorTime(schema, queues, 60).text],
    ['trySetQueueDeletionTime', plans.trySetQueueDeletionTime(schema, queues, 60).text],
    ['trySetCronTime', plans.trySetCronTime(schema, 60)],
    ['trySetBamTime', plans.trySetBamTime(schema, 60)],
    ['insertWarning', plans.insertWarning(schema)],
    ['getWarnings', plans.getWarnings(schema)],
    ['getWarningsCount', plans.getWarningsCount(schema)],
    ['deleteOldWarnings', plans.deleteOldWarnings(schema, 7)],
    ['getBamStatus', plans.getBamStatus(schema)],
    ['getBamEntries', plans.getBamEntries(schema)],
    ['getNextBamCommand', plans.getNextBamCommand(schema)],
    ['setBamCompleted', plans.setBamCompleted(schema, 'bam-id')],
    ['setBamFailed', plans.setBamFailed(schema, 'bam-id', 'bam-error')],
    ['locked single query', plans.locked(schema, plans.getTime())],
    ['locked query array', plans.locked(schema, [plans.getTime(), plans.getVersion(schema)])],
    ['assertMigration', plans.assertMigration(schema, schemaVersion)],
    ['constructionPlans', PgBoss.getConstructionPlans(schema)],
  ]

  for (const migration of migrations) {
    statements.push([
      `migrationPlans from ${migration.previous} to ${schemaVersion}`,
      PgBoss.getMigrationPlans(schema, migration.previous),
    ])
    statements.push([
      `rollbackPlans from ${migration.version}`,
      PgBoss.getRollbackPlans(schema, migration.version),
    ])
  }

  return statements
}

describe('PgQueueDB', () => {
  it('passes pg-boss SQL and positional values through without placeholder rewriting', async () => {
    const query = vi.fn().mockResolvedValue({
      rows: [{ ok: true }],
    })
    const db = new PgQueueDB({ query } as unknown as PgExecutor)

    await expect(db.executeSql('SELECT $1, $2, $3', ['queue-name', undefined, 3])).resolves.toEqual(
      {
        rows: [{ ok: true }],
      }
    )

    expect(query).toHaveBeenCalledWith({
      text: 'SELECT $1, $2, $3',
      values: ['queue-name', null, 3],
    } satisfies PgStatement)
  })

  it('covers pg-boss v12 generated SQL with only pg-compatible placeholders', () => {
    const invalidStatements = collectPgBossSqlSurface().flatMap(([name, sql]) => {
      const stripped = stripSqlStrings(sql)
      const hasQuestionPlaceholder = stripped.includes('?')
      const namedPlaceholders = stripped.match(/(^|[^\w:]):[A-Za-z_][A-Za-z0-9_]*/g) || []

      if (!hasQuestionPlaceholder && namedPlaceholders.length === 0) {
        return []
      }

      return [
        {
          name,
          sql: normalizeSql(sql),
          hasQuestionPlaceholder,
          namedPlaceholders,
        },
      ]
    })

    expect(invalidStatements).toEqual([])
  })
})
