import { DBMigration } from './types'

type MigrationName = keyof typeof DBMigration

interface MigrationResetFloor {
  migration: MigrationName
  activatedBy: MigrationName
}

export const MIGRATION_RESET_FLOORS = [
  {
    // Replaying storage-schema would recreate the legacy unique bucket/name index.
    migration: 'storage-schema',
    activatedBy: 'drop-bucketid-objname-index',
  },
] as const satisfies readonly MigrationResetFloor[]
