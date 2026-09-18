import { DBMigration } from '@internal/database/migrations/types'
import { ERRORS } from '@internal/errors'
import { loadMigrationFiles } from 'postgres-migrations'
import { getConfig } from '../../../config'

const { dbMigrationFreezeAt } = getConfig()

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

/**
 * The highest migration this binary's own code has, ignoring any freeze
 * target. A freeze only governs which migrations this binary will run; it
 * doesn't lower what its own code can already interpret. Callers deciding
 * what this binary's code can safely assume about a schema it didn't
 * migrate itself (for instance a tenant a newer, unfrozen binary already
 * advanced) should clamp to this instead of lastLocalMigrationName.
 */
export async function highestLocalMigrationName() {
  const migrations = await localMigrationFiles()
  const latestMigration = migrations.at(-1)

  if (!latestMigration) {
    throw ERRORS.InternalError(undefined, 'No local migrations found')
  }

  return latestMigration.name as keyof typeof DBMigration
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
