import { DBMigration } from '@internal/database/migrations/types'
import { ERRORS } from '@internal/errors'
import { loadMigrationFiles } from 'postgres-migrations'
import { getConfig } from '../../../config'

const { dbMigrationFreezeAt } = getConfig()
const highestMigrationName = (Object.keys(DBMigration) as (keyof typeof DBMigration)[]).reduce(
  (highest, name) => (DBMigration[name] > DBMigration[highest] ? name : highest)
)

const migrationFilesCache = new Map<string, ReturnType<typeof loadMigrationFiles>>()

export function loadMigrationFilesCached(directory: string) {
  let promise = migrationFilesCache.get(directory)

  if (!promise) {
    promise = loadMigrationFiles(directory).catch((error) => {
      migrationFilesCache.delete(directory)
      throw error
    })
    migrationFilesCache.set(directory, promise)
  }

  return promise
}

export const localMigrationFiles = () => loadMigrationFilesCached('./migrations/tenant')

/** Highest migration this binary knows, ignoring any freeze target. */
export function highestLocalMigrationName() {
  return highestMigrationName
}

export async function lastLocalMigrationName() {
  if (!dbMigrationFreezeAt) {
    return highestLocalMigrationName()
  }

  const migrations = await localMigrationFiles()
  const frozenMigration = migrations.find((m) => m.name === dbMigrationFreezeAt)
  if (!frozenMigration) {
    throw ERRORS.InternalError(undefined, `Migration ${dbMigrationFreezeAt} not found`)
  }
  return frozenMigration.name as keyof typeof DBMigration
}
