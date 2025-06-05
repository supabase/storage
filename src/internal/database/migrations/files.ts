import { DBMigration } from '@internal/database/migrations/types'
import { ERRORS } from '@internal/errors'
import { loadMigrationFiles } from 'postgres-migrations'
import { getConfig } from '../../../config'

const { dbMigrationFreezeAt } = getConfig()

export const loadMigrationFilesCached = memoizePromise(loadMigrationFiles)

export const localMigrationFiles = () => loadMigrationFiles('./migrations/tenant')

export async function lastLocalMigrationName() {
  const migrations = await loadMigrationFilesCached('./migrations/tenant')

  if (!dbMigrationFreezeAt) {
    return migrations[migrations.length - 1].name as keyof typeof DBMigration
  }

  const migrationIndex = migrations.findIndex((m) => m.name === dbMigrationFreezeAt)
  if (migrationIndex === -1) {
    throw ERRORS.InternalError(undefined, `Migration ${dbMigrationFreezeAt} not found`)
  }
  return migrations[migrationIndex].name as keyof typeof DBMigration
}

/**
 * Memoizes a promise
 * @param func
 */
function memoizePromise<T, Args extends unknown[]>(
  func: (...args: Args) => Promise<T>
): (...args: Args) => Promise<T> {
  const cache = new Map<string, Promise<T>>()

  function generateKey(args: Args): string {
    return args
      .map((arg) => {
        if (typeof arg === 'object' && arg !== null) {
          return Object.entries(arg).sort().toString()
        }
        return String(arg)
      })
      .join('|')
  }

  return async function (...args: Args): Promise<T> {
    const key = generateKey(args)
    if (cache.has(key)) {
      return cache.get(key)!
    }

    const result = func(...args)
    cache.set(key, result)
    return result
  }
}
