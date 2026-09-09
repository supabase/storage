import { loadMigrationFiles } from 'postgres-migrations'
import { DBMigration } from './types'

describe('DBMigration', () => {
  it('matches the tenant migration files exactly', async () => {
    const migrations = await loadMigrationFiles('./migrations/tenant')

    expect(Object.entries(DBMigration)).toEqual(migrations.map(({ id, name }) => [name, id]))
  })

  it('adds the shared versioning feature before the lifecycle registry', async () => {
    const migrations = await loadMigrationFiles('./migrations/multitenant')

    expect(migrations.filter(({ id }) => id >= 30).map(({ id, name }) => [id, name])).toEqual([
      [30, 'object-versioning-feature'],
      [31, 'lifecycle-tenants'],
    ])
  })

  it.each([
    {
      migrationName: 'bucket-lifecycle-configuration',
      validationName: 'validate-bucket-lifecycle-constraints',
      constraints: [
        'buckets_lifecycle_configuration_pair_check',
        'buckets_lifecycle_configuration_shape_check',
        'buckets_lifecycle_configuration_standard_only_check',
      ],
    },
    {
      migrationName: 'noncurrent-lifecycle',
      validationName: 'validate-bucket-lifecycle-execution-constraints',
      constraints: [
        'buckets_lifecycle_shard_epoch_check',
        'buckets_lifecycle_shard_count_check',
        'buckets_lifecycle_standard_only_check',
      ],
    },
  ])('stages validation of $migrationName bucket checks', async ({
    migrationName,
    validationName,
    constraints,
  }) => {
    const migrations = await loadMigrationFiles('./migrations/tenant')
    const migration = migrations.find(({ name }) => name === migrationName)
    const validation = migrations.find(({ name }) => name === validationName)

    expect(migration).toBeDefined()
    expect(validation).toBeDefined()
    if (!migration || !validation) {
      throw new Error('Lifecycle migrations were not loaded')
    }

    expect(validation.id).toBeGreaterThan(migration.id)
    expect(migration.sql.match(/\bNOT VALID;/g)).toHaveLength(constraints.length)
    for (const constraint of constraints) {
      expect(migration.sql).not.toContain(`VALIDATE CONSTRAINT ${constraint}`)
      expect(validation.sql).toContain(`VALIDATE CONSTRAINT ${constraint};`)
    }
  })
})
