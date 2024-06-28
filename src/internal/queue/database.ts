import { Db } from 'pg-boss'
import EventEmitter from 'events'
import pg from 'pg'
import { ERRORS } from '@internal/errors'

export class QueueDB extends EventEmitter implements Db {
  opened = false
  isOurs = true

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

  async executeSql(text: string, values: any[]) {
    if (this.opened && this.pool) {
      return await this.pool.query(text, values)
    }

    throw ERRORS.InternalError(undefined, `QueueDB not opened ${this.opened} ${text}`)
  }
}
