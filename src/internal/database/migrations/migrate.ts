import { Client, ClientConfig } from 'pg'
import SQL from 'sql-template-strings'
import { loadMigrationFiles, MigrationError } from 'postgres-migrations'
import { getConfig, MultitenantMigrationStrategy } from '../../../config'
import { logger, logSchema } from '../../monitoring'
import { BasicPgClient, Migration } from 'postgres-migrations/dist/types'
import { validateMigrationHashes } from 'postgres-migrations/dist/validation'
import { runMigration } from 'postgres-migrations/dist/run-migration'
import { searchPath } from '../connection'
import { getTenantConfig, TenantMigrationStatus } from '../tenant'
import { multitenantKnex } from '../multitenant-db'
import { ProgressiveMigrations } from './progressive'
import { ResetMigrationsOnTenant, RunMigrationsOnTenants } from '@storage/events'
import { ERRORS } from '@internal/errors'
import { DBMigration } from './types'
import { getSslSettings } from '../util'
import { MigrationTransformer, DisableConcurrentIndexTransformer } from './migration-transformer'

const {
  multitenantDatabaseUrl,
  pgQueueEnable,
  databaseSSLRootCert,
  dbMigrationStrategy,
  dbAnonRole,
  dbAuthenticatedRole,
  dbSuperUser,
  dbServiceRole,
  dbInstallRoles,
  dbRefreshMigrationHashesOnMismatch,
  dbMigrationFreezeAt,
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

export const progressiveMigrations = new ProgressiveMigrations({
  maxSize: 200,
  interval: 1000 * 5, // 5s
  watch: pgQueueEnable,
})

/**
 * Starts the async migrations depending on the strategy
 * @param signal
 */
export function startAsyncMigrations(signal: AbortSignal) {
  if (signal.aborted) {
    throw ERRORS.Aborted('Migration aborted')
  }
  switch (dbMigrationStrategy) {
    case MultitenantMigrationStrategy.ON_REQUEST:
      return
    case MultitenantMigrationStrategy.PROGRESSIVE:
      progressiveMigrations.start(signal)
      break
    case MultitenantMigrationStrategy.FULL_FLEET:
      runMigrationsOnAllTenants(signal).catch((e) => {
        logger.error(
          {
            type: 'migrations',
            error: e,
          },
          'migration error'
        )
      })
      break
    default:
      throw new Error(`Unknown migration strategy: ${dbMigrationStrategy}`)
  }
}

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
 * List all tenants that needs to have the migrations run
 */
export async function* listTenantsToMigrate(signal: AbortSignal) {
  let lastCursor = 0

  while (true) {
    if (signal.aborted) {
      break
    }

    const migrationVersion = await lastLocalMigrationName()

    const data = await multitenantKnex
      .table<{ id: string; cursor_id: number }>('tenants')
      .select('id', 'cursor_id')
      .where('cursor_id', '>', lastCursor)
      .where((builder) => {
        builder
          .where((whereBuilder) => {
            whereBuilder
              .where('migrations_version', '!=', migrationVersion)
              .whereNotIn('migrations_status', [
                TenantMigrationStatus.FAILED,
                TenantMigrationStatus.FAILED_STALE,
              ])
          })
          .orWhere('migrations_status', null)
      })
      .orderBy('cursor_id', 'asc')
      .limit(200)

    if (data.length === 0) {
      break
    }

    lastCursor = data[data.length - 1].cursor_id
    yield data.map((tenant) => tenant.id)
  }
}

export async function* listTenantsToResetMigrations(
  migration: keyof typeof DBMigration,
  signal: AbortSignal
) {
  let lastCursor = 0

  while (true) {
    if (signal.aborted) {
      break
    }

    const afterMigrations = Object.keys(DBMigration).filter((migrationName) => {
      return DBMigration[migrationName as keyof typeof DBMigration] > DBMigration[migration]
    })

    const data = await multitenantKnex
      .table<{ id: string; cursor_id: number }>('tenants')
      .select('id', 'cursor_id')
      .where('cursor_id', '>', lastCursor)
      .whereIn('migrations_version', afterMigrations)
      .orderBy('cursor_id', 'asc')
      .limit(200)

    if (data.length === 0) {
      break
    }

    lastCursor = data[data.length - 1].cursor_id
    yield data.map((tenant) => tenant.id)
  }
}

/**
 * Update tenant migration version and status
 * @param tenantId
 * @param options
 */
export async function updateTenantMigrationsState(
  tenantId: string,
  options?: { migration?: keyof typeof DBMigration; state: TenantMigrationStatus }
) {
  const migrationVersion = options?.migration || (await lastLocalMigrationName())
  const state = options?.state || TenantMigrationStatus.COMPLETED
  return multitenantKnex
    .table('tenants')
    .where('id', tenantId)
    .update({
      migrations_version: [
        TenantMigrationStatus.FAILED,
        TenantMigrationStatus.FAILED_STALE,
      ].includes(state)
        ? undefined
        : migrationVersion,
      migrations_status: state,
    })
}

/**
 * Determine if a tenant has the migrations up to date
 * @param tenantId
 */
export async function areMigrationsUpToDate(tenantId: string) {
  const latestMigrationVersion = await lastLocalMigrationName()
  const tenant = await getTenantConfig(tenantId)

  return (
    tenant.migrationVersion &&
    DBMigration[latestMigrationVersion] <= DBMigration[tenant.migrationVersion] &&
    tenant.migrationStatus === TenantMigrationStatus.COMPLETED
  )
}

export async function obtainLockOnMultitenantDB<T>(fn: () => Promise<T>) {
  try {
    const result = await multitenantKnex.raw(`SELECT pg_try_advisory_lock(?);`, [
      '-8575985245963000605',
    ])
    const lockAcquired = result.rows.shift()?.pg_try_advisory_lock || false

    if (!lockAcquired) {
      return
    }

    logSchema.info(logger, '[Migrations] Instance acquired the lock', {
      type: 'migrations',
    })

    return await fn()
  } finally {
    try {
      await multitenantKnex.raw(`SELECT pg_advisory_unlock(?);`, ['-8575985245963000605'])
    } catch {}
  }
}

export async function resetMigrationsOnTenants(options: {
  till: keyof typeof DBMigration
  markCompletedTillMigration?: keyof typeof DBMigration
  signal: AbortSignal
}) {
  await obtainLockOnMultitenantDB(async () => {
    logSchema.info(logger, '[Migrations] Listing all tenants', {
      type: 'migrations',
    })

    const tenants = listTenantsToResetMigrations(options.till, options.signal)

    for await (const tenantBatch of tenants) {
      await ResetMigrationsOnTenant.batchSend(
        tenantBatch.map((tenant) => {
          return new ResetMigrationsOnTenant({
            tenantId: tenant,
            untilMigration: options.till,
            markCompletedTillMigration: options.markCompletedTillMigration,
            tenant: {
              host: '',
              ref: tenant,
            },
          })
        })
      )
    }

    logSchema.info(logger, '[Migrations] reset migrations jobs scheduled', {
      type: 'migrations',
    })
  })
}

/**
 * Runs migrations for all tenants
 * only one instance at the time is allowed to run
 */
export async function runMigrationsOnAllTenants(signal: AbortSignal) {
  if (!pgQueueEnable) {
    return
  }
  await obtainLockOnMultitenantDB(async () => {
    logSchema.info(logger, '[Migrations] Listing all tenants', {
      type: 'migrations',
    })
    const tenants = listTenantsToMigrate(signal)
    for await (const tenantBatch of tenants) {
      await RunMigrationsOnTenants.batchSend(
        tenantBatch.map((tenant) => {
          return new RunMigrationsOnTenants({
            tenantId: tenant,
            tenant: {
              host: '',
              ref: tenant,
            },
          })
        })
      )
    }

    logSchema.info(logger, '[Migrations] Async migrations jobs completed', {
      type: 'migrations',
    })
  })
}

/**
 * Runs multi-tenant migrations
 */
export async function runMultitenantMigrations(): Promise<void> {
  logSchema.info(logger, '[Migrations] Running multitenant migrations', {
    type: 'migrations',
  })
  await connectAndMigrate({
    databaseUrl: multitenantDatabaseUrl,
    migrationsDirectory: './migrations/multitenant',
    shouldCreateStorageSchema: false,
    waitForLock: true,
  })
  logSchema.info(logger, '[Migrations] Completed', {
    type: 'migrations',
  })
}

interface MigrateOnTenantOptions {
  databaseUrl: string
  tenantId?: string
  waitForLock?: boolean
  upToMigration?: keyof typeof DBMigration
}

/**
 * Runs migrations on a specific tenant by providing its database DSN
 * @param databaseUrl
 * @param tenantId
 * @param waitForLock
 * @param upToMigration
 */
export async function runMigrationsOnTenant({
  databaseUrl,
  tenantId,
  waitForLock,
  upToMigration,
}: MigrateOnTenantOptions): Promise<void> {
  // default waitForLock to true
  if (typeof waitForLock === 'undefined') {
    waitForLock = true
  }

  await connectAndMigrate({
    databaseUrl,
    migrationsDirectory: './migrations/tenant',
    ssl: getSslSettings({ connectionString: databaseUrl, databaseSSLRootCert }),
    shouldCreateStorageSchema: true,
    tenantId,
    waitForLock,
    upToMigration,
  })
}

export async function resetMigration(options: {
  tenantId: string
  untilMigration: keyof typeof DBMigration
  markCompletedTillMigration?: keyof typeof DBMigration
  databaseUrl: string
}): Promise<boolean> {
  const dbConfig: ClientConfig = {
    connectionString: options.databaseUrl,
    connectionTimeoutMillis: 60_000,
    options: `-c search_path=${searchPath}`,
  }

  dbConfig.ssl = getSslSettings({ connectionString: options.databaseUrl, databaseSSLRootCert })

  const client = await connect(dbConfig)

  try {
    const queryWithAdvisory = withAdvisoryLock(false, async (pgClient) => {
      await pgClient.query(`BEGIN`)

      try {
        await client.query(`SET search_path TO ${searchPath.join(',')}`)

        const migrationsRowsResult = await pgClient.query(`SELECT * from migrations`)
        const currentTenantMigrations = migrationsRowsResult.rows as { id: number; name: string }[]

        if (!currentTenantMigrations.length) {
          return false
        }

        const currentLastMigration = currentTenantMigrations[currentTenantMigrations.length - 1]
        const localMigration = DBMigration[options.untilMigration]

        // This tenant migration is already at the desired migration
        if (currentLastMigration.id === localMigration) {
          return false
        }

        // This tenant migration is behind of the desired migration
        if (currentLastMigration.id < localMigration) {
          return false
        }

        // This tenant migration is ahead the desired migration
        await pgClient.query(SQL`DELETE FROM migrations WHERE id > ${localMigration}`)

        // latest run migration
        let latestRunMigration = options.untilMigration

        // If we want to prevent the migrations to run in the future
        // we need to update the tenant migration state
        if (options.markCompletedTillMigration) {
          const markCompletedTillMigration = DBMigration[options.markCompletedTillMigration]

          const aheadMigrations = Object.keys(DBMigration).filter((migrationName) => {
            return (
              DBMigration[migrationName as keyof typeof DBMigration] >
                DBMigration[options.untilMigration] &&
              DBMigration[migrationName as keyof typeof DBMigration] <= markCompletedTillMigration
            )
          })

          if (aheadMigrations.length) {
            const localFileMigrations = await loadMigrationFilesCached('./migrations/tenant')

            const query = SQL`INSERT INTO `
              .append('migrations')
              .append('(id, name, hash, executed_at) VALUES ')

            aheadMigrations.forEach((migrationName, index) => {
              const migration = localFileMigrations.find(
                (m) => m.id === DBMigration[migrationName as keyof typeof DBMigration]
              )

              if (!migration) {
                throw Error(`Migration ${migrationName} not found`)
              }

              query.append(SQL`(${migration.id}, ${migration.name}, ${migration.hash}, NOW())`)
              if (index !== aheadMigrations.length - 1) {
                query.append(',')
              }
            })

            await pgClient.query(query)

            latestRunMigration = options.markCompletedTillMigration
          }
        }

        await updateTenantMigrationsState(options.tenantId, {
          migration: latestRunMigration,
          state: TenantMigrationStatus.COMPLETED,
        })

        await pgClient.query(`COMMIT`)

        return true
      } catch (e) {
        await pgClient.query(`ROLLBACK`)
        throw e
      }
    })

    return await queryWithAdvisory(client)
  } finally {
    await client.end()
  }
}

/**
 * Connect to the database
 * @param options
 */
async function connect(options: {
  connectionString?: string | undefined
  ssl?: ClientConfig['ssl']
  tenantId?: string
}) {
  const { ssl, tenantId, connectionString } = options

  const dbConfig: ClientConfig = {
    connectionString: connectionString,
    connectionTimeoutMillis: 60_000,
    options: `-c search_path=${searchPath}`,
    ssl,
  }

  const client = new Client(dbConfig)
  client.on('error', (err) => {
    logSchema.error(logger, 'Error on database connection', {
      type: 'error',
      error: err,
      project: tenantId,
    })
  })
  await client.connect()
  return client
}

/**
 * Connect and migrate the database
 * @param options
 */
async function connectAndMigrate(options: {
  databaseUrl: string | undefined
  migrationsDirectory: string
  ssl?: ClientConfig['ssl']
  shouldCreateStorageSchema?: boolean
  tenantId?: string
  waitForLock?: boolean
  upToMigration?: keyof typeof DBMigration
}) {
  const { shouldCreateStorageSchema, migrationsDirectory, ssl, databaseUrl, waitForLock } = options

  const dbConfig: ClientConfig = {
    connectionString: databaseUrl,
    connectionTimeoutMillis: 60_000,
    options: `-c search_path=${searchPath}`,
    statement_timeout: 1000 * 60 * 60 * 12, // 12 hours
    ssl,
  }

  const client = await connect(dbConfig)

  try {
    await client.query(`SET statement_timeout TO '12h'`)
    await migrate({
      client,
      migrationsDirectory,
      waitForLock: Boolean(waitForLock),
      shouldCreateStorageSchema,
      upToMigration: options.upToMigration,
    })
  } finally {
    await client.end()
  }
}

interface MigrateOptions {
  client: BasicPgClient
  migrationsDirectory: string
  waitForLock: boolean
  shouldCreateStorageSchema?: boolean
  upToMigration?: keyof typeof DBMigration
}

/**
 * Migration runner with advisory lock
 * @param dbConfig
 * @param migrationsDirectory
 * @param waitForLock
 * @param shouldCreateStorageSchema
 */
export async function migrate({
  client,
  migrationsDirectory,
  waitForLock,
  shouldCreateStorageSchema,
  upToMigration,
}: MigrateOptions): Promise<Array<Migration>> {
  const accessMethod = await getDefaultAccessMethod(client)
  return withAdvisoryLock(
    waitForLock,
    runMigrations({
      migrationsDirectory,
      shouldCreateStorageSchema,
      upToMigration,
      // Remove concurrent index creation if we're using oriole db as it does not support it currently
      transformers: accessMethod === 'orioledb' ? [new DisableConcurrentIndexTransformer()] : [],
    })
  )(client)
}

interface RunMigrationOptions {
  migrationsDirectory: string
  shouldCreateStorageSchema?: boolean
  upToMigration?: keyof typeof DBMigration
  transformers?: MigrationTransformer[]
}

/**
 * Run Migration from a specific directory
 * @param migrationsDirectory
 * @param shouldCreateStorageSchema
 * @param upToMigration
 */
function runMigrations({
  migrationsDirectory,
  shouldCreateStorageSchema,
  upToMigration,
  transformers = [],
}: RunMigrationOptions) {
  return async (client: BasicPgClient) => {
    let intendedMigrations = await loadMigrationFilesCached(migrationsDirectory)
    let lastMigrationId = intendedMigrations[intendedMigrations.length - 1].id

    if (upToMigration) {
      const migrationIndex = intendedMigrations.findIndex((m) => m.name === upToMigration)
      if (migrationIndex === -1) {
        throw ERRORS.InternalError(undefined, `Migration ${dbMigrationFreezeAt} not found`)
      }
      intendedMigrations = intendedMigrations.slice(0, migrationIndex + 1)
      lastMigrationId = intendedMigrations[migrationIndex].id
    }

    try {
      const migrationTableName = 'migrations'

      await client.query(`SET search_path TO ${searchPath.join(',')}`)

      let appliedMigrations: Migration[] = []
      if (await doesTableExist(client, migrationTableName)) {
        const selectQueryCurrentMigration = SQL`SELECT * FROM `
          .append(migrationTableName)
          .append(SQL` WHERE id <= ${lastMigrationId}`)

        const { rows } = await client.query(selectQueryCurrentMigration)
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
        const result = await runMigration(
          migrationTableName,
          client
        )(runMigrationTransformers(migration, transformers))
        completedMigrations.push(result)
      }

      return completedMigrations
    } catch (e) {
      const error: MigrationError = new Error(`Migration failed. Reason: ${(e as Error).message}`)
      error.cause = e + ''
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
 * Transforms provided migration by running all transformers
 * @param migration
 * @param transformers
 */
function runMigrationTransformers(
  migration: Migration,
  transformers: MigrationTransformer[]
): Migration {
  for (const transformer of transformers) {
    migration = transformer.transform(migration)
  }
  return migration
}

/**
 * Get the current default access method for this database
 * @param client
 */
async function getDefaultAccessMethod(client: BasicPgClient): Promise<string> {
  const result = await client.query(`SHOW default_table_access_method`)
  return result.rows?.[0]?.default_table_access_method || ''
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
 * @param waitForLock
 * @param f
 */
function withAdvisoryLock<T>(
  waitForLock: boolean,
  f: (client: BasicPgClient) => Promise<T>
): (client: BasicPgClient) => Promise<T> {
  return async (client: BasicPgClient): Promise<T> => {
    try {
      try {
        let acquired = false
        let tries = 1

        const timeout = 3000
        const start = Date.now()

        while (!acquired) {
          const elapsed = Date.now() - start
          if (elapsed > timeout) {
            throw ERRORS.LockTimeout()
          }

          const lockResult = await client.query(
            'SELECT pg_try_advisory_lock(-8525285245963000605);'
          )
          if (lockResult.rows[0].pg_try_advisory_lock === true) {
            acquired = true
          } else {
            if (waitForLock) {
              await new Promise((res) => setTimeout(res, 20 * tries))
            } else {
              throw ERRORS.LockTimeout()
            }
          }

          tries++
        }
      } catch (e) {
        throw e
      }

      return await f(client)
    } catch (e) {
      throw e
    } finally {
      try {
        await client.query('SELECT pg_advisory_unlock(-8525285245963000605);')
      } catch {}
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

  return invalidHashes
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
    const afterMigration = newMigrations.slice(migration.index).map((m) => ({
      ...m,
      id: m.id + 1,
      hash: intendedMigrations[m.id].hash,
    }))

    migrations.push(...afterMigration)
    newMigrations = migrations
    shouldUpdateMigrations = true
  })

  if (shouldUpdateMigrations) {
    await client.query(`BEGIN`)
    try {
      await client.query(`DELETE FROM ${migrationTableName} WHERE id is not NULL`)
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
