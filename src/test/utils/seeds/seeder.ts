import { KnexPersistence } from '@internal/testing/seeder'
import { BucketsSeeder } from './bucket'
import { Knex } from 'knex'

export class TestUtils {
  private persistence: KnexPersistence
  private bucketsSeeder: BucketsSeeder

  constructor(knexConfig: Knex.Config | Knex) {
    this.persistence = new KnexPersistence(knexConfig)
    this.bucketsSeeder = new BucketsSeeder(this.persistence)
  }

  get buckets(): BucketsSeeder {
    return this.bucketsSeeder
  }

  /**
   * Runs the seeding operations:
   * 1. Executes the provided seeding function within a transaction.
   * 2. After seeding, performs batch inserts for all collected records.
   * @param operation - The seeding operations to execute.
   */
  async runSeeder(operation: () => Promise<void>): Promise<void> {
    await this.bucketsSeeder.runInTransaction(async () => {
      await operation()
    })
    await this.bucketsSeeder.batchInsertAll()
  }

  /**
   * Destroys the Knex instance to close database connections.
   */
  async destroy(): Promise<void> {
    await this.persistence.destroy()
  }
}
