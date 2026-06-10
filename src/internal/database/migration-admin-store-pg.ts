import { QueryResultRow } from 'pg'
import { PgExecutor } from './pg-connection'
import { quoteIdentifier } from './sql'
import { TenantCursorRow } from './tenant-store-pg'

export class MigrationAdminStorePg {
  private readonly jobTable: string

  constructor(
    private db: PgExecutor,
    pgBossSchema: string
  ) {
    this.jobTable = `${quoteIdentifier(pgBossSchema)}.job`
  }

  async listActiveJobs(queueName: string, limit: number): Promise<QueryResultRow[]> {
    const result = await this.query({
      text: `
        SELECT *
        FROM ${this.jobTable}
        WHERE state = 'active'
          AND name = $1
        ORDER BY created_on DESC
        LIMIT $2
      `,
      values: [queueName, limit],
    })

    return result.rows
  }

  async completeActiveJobs(queueName: string): Promise<number> {
    const result = await this.query({
      text: `
        UPDATE ${this.jobTable} AS job
        SET state = 'completed'
        WHERE state = 'active'
          AND name = $1
      `,
      values: [queueName],
    })

    return result.rowCount || 0
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

  async deleteTenantJobs(tenantId: string, queueName: string): Promise<number> {
    const result = await this.query({
      text: `
        DELETE FROM ${this.jobTable}
        WHERE data->'tenant'->>'ref' = $1
          AND name = $2
      `,
      values: [tenantId, queueName],
    })

    return result.rowCount || 0
  }

  async listFailedTenants(offset: number, limit: number): Promise<TenantCursorRow[]> {
    const result = await this.query<TenantCursorRow>({
      text: `
        SELECT id, cursor_id
        FROM tenants
        WHERE migrations_status = 'FAILED'
          AND cursor_id > $1
        ORDER BY cursor_id ASC
        LIMIT $2
      `,
      values: [offset, limit],
    })

    return result.rows
  }

  private query<T extends QueryResultRow = QueryResultRow>(
    statement: Parameters<PgExecutor['query']>[0]
  ) {
    return this.db.query<T>(statement)
  }
}
