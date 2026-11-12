import { QueryResultRow } from 'pg'
import type { DatabaseExecutor } from './connection'
import { quoteIdentifier } from './postgres/sql'

export const MIGRATION_ADMIN_JOB_LIMIT = 2000

export class MigrationAdminStorePg {
  private readonly jobTable: string

  constructor(
    private db: DatabaseExecutor,
    pgBossSchema: string
  ) {
    this.jobTable = `${quoteIdentifier(pgBossSchema)}.job`
  }

  async listTenantJobs(
    tenantId: string,
    queueName: string,
    limit: number
  ): Promise<QueryResultRow[]> {
    const result = await this.query({
      text: `
        SELECT *
        FROM ${this.jobTable}
        WHERE data->'tenant'->>'ref' = $1
          AND name = $2
        ORDER BY created_on DESC
        LIMIT $3
      `,
      values: [tenantId, queueName, limit],
    })

    return result.rows
  }

  async deleteTenantJobs(tenantId: string, queueName: string, limit: number): Promise<number> {
    const result = await this.query({
      text: `
        WITH jobs_to_delete AS (
          SELECT id
          FROM ${this.jobTable}
          WHERE data->'tenant'->>'ref' = $1
            AND name = $2
          ORDER BY created_on DESC
          LIMIT $3
        )
        DELETE FROM ${this.jobTable} AS job
        USING jobs_to_delete
        WHERE job.id = jobs_to_delete.id
      `,
      values: [tenantId, queueName, limit],
    })

    return result.rowCount || 0
  }

  private query<T extends QueryResultRow = QueryResultRow>(
    statement: Parameters<DatabaseExecutor['query']>[0]
  ) {
    return this.db.query<T>(statement)
  }
}
