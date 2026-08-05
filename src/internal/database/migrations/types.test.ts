import { loadMigrationFiles } from 'postgres-migrations'
import { DBMigration } from './types'

describe('DBMigration', () => {
  it('matches the tenant migration files exactly', async () => {
    const migrations = await loadMigrationFiles('./migrations/tenant')

    expect(Object.entries(DBMigration)).toEqual(migrations.map(({ id, name }) => [name, id]))
  })
})
