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
import { RunMigrationsOnTenants } from '@storage/events'
import { ERRORS } from '@internal/errors'
import { DBMigration } from './types'

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

export async function lastMigrationName() {
  const migrations = await loadMigrationFilesCached('./migrations/tenant')
  return migrations[migrations.length - 1].name as keyof typeof DBMigration
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

    const migrationVersion = await lastMigrationName()

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

/**
 * Update tenant migration version and status
 * @param tenantId
 * @param options
 */
export async function updateTenantMigrationsState(
  tenantId: string,
  options?: { state: TenantMigrationStatus }
) {
  const migrationVersion = await lastMigrationName()
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
  const latestMigrationVersion = await lastMigrationName()
  const tenant = await getTenantConfig(tenantId)

  return (
    latestMigrationVersion === tenant.migrationVersion &&
    tenant.migrationStatus === TenantMigrationStatus.COMPLETED
  )
}

export async function hasMissingSyncMigration(tenantId: string) {
  const { migrationVersion, migrationStatus } = await getTenantConfig(tenantId)
  const migrations = await loadMigrationFilesCached('./migrations/tenant')

  if (!migrationStatus) {
    return migrations.some((m) => {
      return m.contents.includes('---SYNC---')
    })
  }

  const indexLastMigration = migrations.findIndex((m) => m.name === migrationVersion)

  if (indexLastMigration === -1) {
    return true
  }

  const migrationAfterLast = migrations.slice(indexLastMigration + 1)
  return migrationAfterLast.some((m) => {
    return m.contents.includes('---SYNC---')
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

  try {
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
  } finally {
    try {
      await multitenantKnex.raw(`SELECT pg_advisory_unlock(?);`, ['-8575985245963000605'])
    } catch (e) {}
  }
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

/**
 * Runs migrations on a specific tenant by providing its database DSN
 * @param databaseUrl
 * @param tenantId
 * @param waitForLock
 */
export async function runMigrationsOnTenant(
  databaseUrl: string,
  tenantId?: string,
  waitForLock = true
): Promise<void> {
  let ssl: ClientConfig['ssl'] | undefined = undefined

  if (databaseSSLRootCert) {
    ssl = { ca: databaseSSLRootCert }
  }

  await connectAndMigrate({
    databaseUrl,
    migrationsDirectory: './migrations/tenant',
    ssl,
    shouldCreateStorageSchema: true,
    tenantId,
    waitForLock,
  })
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
}) {
  const {
    shouldCreateStorageSchema,
    migrationsDirectory,
    ssl,
    tenantId,
    databaseUrl,
    waitForLock,
  } = options

  const dbConfig: ClientConfig = {
    connectionString: databaseUrl,
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
  try {
    await client.connect()
    await migrate({ client }, migrationsDirectory, Boolean(waitForLock), shouldCreateStorageSchema)
  } finally {
    await client.end()
  }
}

/**
 * Migration runner with advisory lock
 * @param dbConfig
 * @param migrationsDirectory
 * @param waitForLock
 * @param shouldCreateStorageSchema
 */
export async function migrate(
  dbConfig: { client: BasicPgClient },
  migrationsDirectory: string,
  waitForLock: boolean,
  shouldCreateStorageSchema?: boolean
): Promise<Array<Migration>> {
  return withAdvisoryLock(
    waitForLock,
    runMigrations(migrationsDirectory, shouldCreateStorageSchema)
  )(dbConfig.client)
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

      await client.query(`SET search_path TO ${searchPath.join(',')}`)

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
              return [] as unknown as Promise<T>
            }
          }

          tries++
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
