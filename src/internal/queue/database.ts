import { Db } from 'pg-boss'
import pg from 'pg'
import { ERRORS } from '@internal/errors'
import { Knex } from 'knex'
import EventEmitter from 'node:events'

export class QueueDB extends EventEmitter implements Db {
  opened = false
  isOurs = true
  events = {
    error: 'error',
  }
  protected config: pg.PoolConfig
  protected pool?: pg.Pool

  constructor(config: pg.PoolConfig) {
    super()

    config.application_name = config.application_name || 'pgboss'

    this.config = config
  }

  async open() {
    this.pool = new pg.Pool({ ...this.config, min: 0 })
    this.pool.on('error', (error) => this.emit('error', error))

    this.opened = true
  }

  async close() {
    this.opened = false
    await this.pool?.end()
  }

  protected async useTransaction<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
    if (!this.opened || !this.pool) {
      throw ERRORS.InternalError(undefined, `QueueDB not opened ${this.opened}`)
    }

    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')

      if (this.config.statement_timeout && this.config.statement_timeout > 0) {
        await client.query(`SET LOCAL statement_timeout = ${this.config.statement_timeout}`)
      }

      const result = await fn(client)
      await client.query('COMMIT')
      return result
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  async executeSql(text: string, values: any[]) {
    if (this.opened && this.pool) {
      return this.useTransaction((client) => client.query(text, values))
    }

    throw ERRORS.InternalError(undefined, `QueueDB not opened ${this.opened} ${text}`)
  }
}

export class KnexQueueDB extends EventEmitter implements Db {
  events = {
    error: 'error',
  }

  constructor(protected readonly knex: Knex) {
    super()
  }

  async executeSql(text: string, values: any[]): Promise<{ rows: any[] }> {
    const knexQuery = text.replaceAll('$', ':')
    const params: Record<string, any> = {}

    values.forEach((value, index) => {
      const key = (index + 1).toString()
      params[key] = value === undefined ? null : value
    })
    const result = await this.knex.raw(knexQuery, params)
    return { rows: result.rows }
  }
}
