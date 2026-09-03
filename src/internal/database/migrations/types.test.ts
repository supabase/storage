import { loadMigrationFiles } from 'postgres-migrations'
import { ROUTE_OPERATIONS } from '../../../http/routes/operations'
import { DBMigration } from './types'

describe('DBMigration', () => {
  it('matches the tenant migration files exactly', async () => {
    const migrations = await loadMigrationFiles('./migrations/tenant')

    expect(Object.entries(DBMigration)).toEqual(migrations.map(({ id, name }) => [name, id]))
  })

  it('stages validation of the lifecycle bucket checks', async () => {
    const migrations = await loadMigrationFiles('./migrations/tenant')
    const migration = migrations.find(({ name }) => name === 'bucket-lifecycle-configuration')
    const validation = migrations.find(
      ({ name }) => name === 'validate-bucket-lifecycle-constraints'
    )

    expect(migration).toBeDefined()
    expect(validation).toBeDefined()
    if (!migration || !validation) {
      throw new Error('Lifecycle migrations were not loaded')
    }

    expect(migration.sql.match(/\bNOT VALID;/g)).toHaveLength(3)
    for (const constraint of [
      'buckets_lifecycle_configuration_pair_check',
      'buckets_lifecycle_configuration_shape_check',
      'buckets_lifecycle_configuration_standard_only_check',
    ]) {
      expect(migration.sql).not.toContain(`VALIDATE CONSTRAINT ${constraint}`)
      expect(validation.sql).toContain(`VALIDATE CONSTRAINT ${constraint};`)
    }
  })

  it('keeps the lifecycle trigger operations aligned with route names', async () => {
    const migrations = await loadMigrationFiles('./migrations/tenant')
    const operations = [
      ROUTE_OPERATIONS.PUT_BUCKET_LIFECYCLE,
      ROUTE_OPERATIONS.S3_PUT_BUCKET_LIFECYCLE,
      ROUTE_OPERATIONS.DELETE_BUCKET_LIFECYCLE,
      ROUTE_OPERATIONS.S3_DELETE_BUCKET_LIFECYCLE,
    ]

    const migration = migrations.find(({ name }) => name === 'bucket-lifecycle-configuration')
    expect(migration).toBeDefined()
    if (!migration) {
      throw new Error('Lifecycle configuration migration was not loaded')
    }

    for (const operation of operations) {
      expect(migration.sql).toContain(`'${operation}'`)
    }
  })
})
