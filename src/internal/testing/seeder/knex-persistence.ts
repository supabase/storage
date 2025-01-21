// src/persistence/KnexPersistence.ts
import { Persistence } from './persistence'
import knex, { Knex } from 'knex'

export class KnexPersistence implements Persistence {
  private knex: Knex
  private trx: Knex.Transaction | null = null

  constructor(knexConfig: Knex.Config) {
    this.knex = knex(knexConfig)
  }

  async insertBatch<T>(table: string, records: T[]): Promise<void> {
    if (this.trx) {
      await this.trx(table).insert(records)
    } else {
      await this.knex(table).insert(records)
    }
  }

  async beginTransaction(): Promise<void> {
    this.trx = await this.knex.transaction()
  }

  async commitTransaction(): Promise<void> {
    if (this.trx) {
      await this.trx.commit()
      this.trx = null
    }
  }

  async rollbackTransaction(): Promise<void> {
    if (this.trx) {
      await this.trx.rollback()
      this.trx = null
    }
  }

  async rawQuery(query: string, bindings: any[] = []): Promise<any> {
    if (this.trx) {
      return this.trx.raw(query, bindings)
    }
    return this.knex.raw(query, bindings)
  }

  // Optional: Destroy the Knex instance to close connections
  async destroy(): Promise<void> {
    await this.knex.destroy()
  }
}
