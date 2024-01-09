import { Client, ClientConfig } from 'pg'
import { loadMigrationFiles, MigrationError } from 'postgres-migrations'
import { getConfig } from '../config'
import { logger } from '../monitoring'
import { BasicPgClient, Migration } from 'postgres-migrations/dist/types'
import { validateMigrationHashes } from 'postgres-migrations/dist/validation'
import { runMigration } from 'postgres-migrations/dist/run-migration'
import SQL from 'sql-template-strings'
import { searchPath } from './connection'

const {
  multitenantDatabaseUrl,
  databaseSSLRootCert,
  dbAnonRole,
  dbAuthenticatedRole,
  dbSuperUser,
  dbServiceRole,
  dbInstallRoles,
  dbRefreshMigrationHashesOnMismatch,
} = getConfig()

const loadMigrationFilesCached = memoizePromise(loadMigrationFiles)

/**
 * Migrations that were added after the initial release
 */
const backportMigrations = [
  {
    index: 2,
    from: 'pathtoken-column',
    to: 'storage-schema',
  },
]

/**
 * Runs multi-tenant migrations
 */
export async function runMultitenantMigrations(): Promise<void> {
  logger.info('running multitenant migrations')
  await connectAndMigrate(multitenantDatabaseUrl, './migrations/multitenant', undefined, false)
  logger.info('finished multitenant migrations')
}

/**
 * Runs migrations on a specific tenant by providing its database DSN
 * @param databaseUrl
 */
export async function runMigrationsOnTenant(databaseUrl: string): Promise<void> {
  let ssl: ClientConfig['ssl'] | undefined = undefined

  if (databaseSSLRootCert) {
    ssl = { ca: databaseSSLRootCert }
  }

  await connectAndMigrate(databaseUrl, './migrations/tenant', ssl)
}

/**
 * Connect and migrate the database
 * @param databaseUrl
 * @param migrationsDirectory
 * @param ssl
 * @param shouldCreateStorageSchema
 */
async function connectAndMigrate(
  databaseUrl: string | undefined,
  migrationsDirectory: string,
  ssl?: ClientConfig['ssl'],
  shouldCreateStorageSchema?: boolean
) {
  const dbConfig: ClientConfig = {
    connectionString: databaseUrl,
    connectionTimeoutMillis: 10_000,
    options: `-c search_path=${searchPath}`,
    ssl,
  }

  const client = new Client(dbConfig)
  try {
    await client.connect()
    await migrate({ client }, migrationsDirectory, shouldCreateStorageSchema)
  } finally {
    await client.end()
  }
}

/**
 * Migration runner with advisory lock
 * @param dbConfig
 * @param migrationsDirectory
 * @param shouldCreateStorageSchema
 */
export async function migrate(
  dbConfig: { client: BasicPgClient },
  migrationsDirectory: string,
  shouldCreateStorageSchema?: boolean
): Promise<Array<Migration>> {
  return withAdvisoryLock(runMigrations(migrationsDirectory, shouldCreateStorageSchema))(
    dbConfig.client
  )
}

/**
 * Run Migration from a specific directory
 * @param migrationsDirectory
 * @param shouldCreateStorageSchema
 */
function runMigrations(migrationsDirectory: string, shouldCreateStorageSchema = true) {
  return async (client: BasicPgClient) => {
    const intendedMigrations = await loadMigrationFilesCached(migrationsDirectory)

    try {
      const migrationTableName = 'migrations'

      let appliedMigrations: Migration[] = []
      if (await doesTableExist(client, migrationTableName)) {
        const { rows } = await client.query(`SELECT * FROM ${migrationTableName} ORDER BY id`)
        appliedMigrations = rows

        if (rows.length > 0) {
          appliedMigrations = await refreshMigrationPosition(
            client,
            migrationTableName,
            appliedMigrations,
            intendedMigrations
          )
        }
      } else if (shouldCreateStorageSchema) {
        const schemaExists = await doesSchemaExists(client, 'storage')
        if (!schemaExists) {
          await client.query(`CREATE SCHEMA IF NOT EXISTS storage`)
        }
      }

      try {
        validateMigrationHashes(intendedMigrations, appliedMigrations)
      } catch (e) {
        if (!dbRefreshMigrationHashesOnMismatch) {
          throw e
        }

        await refreshMigrationHash(
          client,
          migrationTableName,
          intendedMigrations,
          appliedMigrations
        )
      }

      const migrationsToRun = filterMigrations(intendedMigrations, appliedMigrations)
      const completedMigrations = []

      if (migrationsToRun.length > 0) {
        await client.query(SQL`SELECT 
          set_config('storage.install_roles', ${dbInstallRoles}, false),
          set_config('storage.anon_role', ${dbAnonRole}, false),
          set_config('storage.authenticated_role', ${dbAuthenticatedRole}, false),
          set_config('storage.service_role', ${dbServiceRole}, false),
          set_config('storage.super_user', ${dbSuperUser}, false)
        `)
      }

      for (const migration of migrationsToRun) {
        const result = await runMigration(migrationTableName, client)(migration)
        completedMigrations.push(result)
      }

      return completedMigrations
    } catch (e: any) {
      const error: MigrationError = new Error(`Migration failed. Reason: ${e.message}`)
      error.cause = e
      throw error
    }
  }
}

/**
 * Filter migrations that have not been applied yet
 * @param migrations
 * @param appliedMigrations
 */
function filterMigrations(
  migrations: Array<Migration>,
  appliedMigrations: Record<number, Migration | undefined>
) {
  const notAppliedMigration = (migration: Migration) => !appliedMigrations[migration.id]

  return migrations.filter(notAppliedMigration)
}

/**
 * Checks if a table exists
 * @param client
 * @param tableName
 */
async function doesTableExist(client: BasicPgClient, tableName: string) {
  const result = await client.query(SQL`SELECT EXISTS (
  SELECT 1
  FROM   pg_catalog.pg_class c
  WHERE  c.relname = ${tableName}
  AND    c.relkind = 'r'
);`)

  return result.rows.length > 0 && result.rows[0].exists
}

/**
 * Check if schema exists
 * @param client
 * @param schemaName
 */
async function doesSchemaExists(client: BasicPgClient, schemaName: string) {
  const result = await client.query(SQL`SELECT EXISTS (
      SELECT 1
      FROM information_schema.schemata
      WHERE schema_name = ${schemaName}
  );`)

  return result.rows.length > 0 && result.rows[0].exists === 'true'
}

/**
 * Wraps a function with an advisory lock
 * @param f
 */
function withAdvisoryLock<T>(
  f: (client: BasicPgClient) => Promise<T>
): (client: BasicPgClient) => Promise<T> {
  return async (client: BasicPgClient): Promise<T> => {
    try {
      try {
        let acquired = false
        while (!acquired) {
          const lockResult = await client.query(
            'SELECT pg_try_advisory_lock(-8525285245963000605);'
          )
          if (lockResult.rows[0].pg_try_advisory_lock === true) {
            acquired = true
          } else {
            await new Promise((res) => setTimeout(res, 700))
          }
        }
      } catch (e) {
        throw e
      }

      const result = await f(client)
      return result
    } catch (e) {
      throw e
    } finally {
      try {
        await client.query('SELECT pg_advisory_unlock(-8525285245963000605);')
      } catch (e) {}
    }
  }
}

async function refreshMigrationHash(
  client: BasicPgClient,
  migrationTableName: string,
  intendedMigrations: Migration[],
  appliedMigrations: Migration[]
) {
  const invalidHash = (migration: Migration) => {
    const appliedMigration = appliedMigrations[migration.id]
    return appliedMigration != null && appliedMigration.hash !== migration.hash
  }

  // Assert migration hashes are still same
  const invalidHashes = intendedMigrations.filter(invalidHash)

  if (invalidHashes.length > 0) {
    await client.query('BEGIN')

    try {
      await Promise.all(
        invalidHashes.map((migration) => {
          const query = SQL`UPDATE `
            .append(migrationTableName)
            .append(SQL` SET hash = ${migration.hash} WHERE id = ${migration.id}`)

          return client.query(query)
        })
      )
      await client.query('COMMIT')
    } catch (e) {
      await client.query('ROLLBACK')
      throw e
    }
  }
}

/**
 * Backports migrations that were added after the initial release
 *
 * @param client
 * @param migrationTableName
 * @param appliedMigrations
 * @param intendedMigrations
 */
async function refreshMigrationPosition(
  client: BasicPgClient,
  migrationTableName: string,
  appliedMigrations: Migration[],
  intendedMigrations: Migration[]
) {
  let newMigrations = [...appliedMigrations]
  let shouldUpdateMigrations = false

  backportMigrations.forEach((migration) => {
    const existingMigration = newMigrations?.[migration.index]

    if (!existingMigration || (existingMigration && existingMigration.name !== migration.from)) {
      return
    }

    // slice till the migration we want to backport
    const migrations = newMigrations.slice(0, migration.index)

    // add the migration we want to backport
    migrations.push(intendedMigrations[migration.index])

    // add the other run migrations by updating their id and hash
    const afterMigration = newMigrations.slice(migration.index).map((m) => {
      ;(m as any).id = m.id + 1
      ;(m as any).hash = intendedMigrations[m.id].hash
      return m
    })

    migrations.push(...afterMigration)
    newMigrations = migrations
    shouldUpdateMigrations = true
  })

  if (shouldUpdateMigrations) {
    await client.query(`BEGIN`)
    try {
      await client.query(`DELETE FROM ${migrationTableName}`)
      const query = SQL`INSERT INTO `
        .append(migrationTableName)
        .append('(id, name, hash, executed_at) VALUES ')

      newMigrations.forEach((migration, index) => {
        query.append(SQL`(${migration.id}, ${migration.name}, ${migration.hash}, NOW())`)
        if (index !== newMigrations.length - 1) {
          query.append(',')
        }
      })
      await client.query(query)
      await client.query(`COMMIT`)
    } catch (e) {
      await client.query(`ROLLBACK`)
      throw e
    }
  }

  return newMigrations
}

/**
 * Memoizes a promise
 * @param func
 */
function memoizePromise<T, Args extends any[]>(
  func: (...args: Args) => Promise<T>
): (...args: Args) => Promise<T> {
  const cache = new Map<string, Promise<T>>()

  function generateKey(args: Args): string {
    return args
      .map((arg) => {
        if (typeof arg === 'object') {
          return Object.entries(arg).sort().toString()
        }
        return arg.toString()
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
