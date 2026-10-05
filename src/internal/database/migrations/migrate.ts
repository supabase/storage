import { ERRORS } from '@internal/errors'
import { ResetMigrationsOnTenant, RunMigrationsOnTenants } from '@storage/events'
import { Client, ClientConfig } from 'pg'
import { MigrationError } from 'postgres-migrations'
import { runMigration } from 'postgres-migrations/dist/run-migration'
import { BasicPgClient, Migration } from 'postgres-migrations/dist/types'
import { validateMigrationHashes } from 'postgres-migrations/dist/validation'
import SQL from 'sql-template-strings'
import { getConfig, MultitenantMigrationStrategy } from '../../../config'
import { logger, logSchema } from '../../monitoring'
import type { DatabaseTransaction } from '../connection'
import { multitenantPgExecutor } from '../multitenant-pg'
import { searchPath } from '../pool'
import { getSslSettings } from '../postgres/ssl'
import { getTenantConfig, TenantMigrationStatus } from '../tenant'
import {
  type CompleteMigrationsOptions,
  type FailMigrationsOptions,
  TenantConfigStorePg,
} from '../tenant-store-pg'
import { deriveVectorDatabaseUrl, VECTOR_DATABASE_NAME } from '../vector-store-url'
import { repairInvalidConcurrentIndexes } from './concurrent-index-guard'
import {
  highestLocalMigrationName,
  lastLocalMigrationName,
  loadMigrationFilesCached,
  localMigrationFiles,
} from './files'
import { isDBMigrationName, isUnrecognizedMigration } from './guards'
import { getCachedTenantMigration, needsObservation, observeTenantMigration } from './observed'
import { ProgressiveMigrations } from './progressive'
import { MIGRATION_RESET_FLOORS } from './reset-floor'
import { DisableConcurrentIndexTransformer, MigrationTransformer } from './transformers'
import { DBMigration } from './types'

const {
  isMultitenant,
  multitenantDatabaseUrl,
  pgQueueEnable,
  databaseSSLRootCert,
  databaseConnectionTimeout,
  databaseStatementTimeout,
  dbMigrationStrategy,
  dbAnonRole,
  dbAuthenticatedRole,
  dbSuperUser,
  dbServiceRole,
  dbInstallRoles,
  dbRefreshMigrationHashesOnMismatch,
  dbMigrationFreezeAt,
  icebergShards,
  vectorBucketProvider,
  vectorDatabaseCreate,
  vectorStoreMigrationsEnabled,
  vectorDatabaseURL,
} = getConfig()

const tenantConfigStorePg = new TenantConfigStorePg(multitenantPgExecutor)

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
    case MultitenantMigrationStrategy.PROGRESSIVE:
      progressiveMigrations.start(signal)
      break
    case MultitenantMigrationStrategy.FULL_FLEET: {
      progressiveMigrations.start(signal)
      const fullFleetMigrationRun = runMigrationsOnAllTenants({ signal }).catch((e) => {
        logger.error(
          {
            type: 'migrations',
            error: e,
          },
          'migration error'
        )
      })
      signal.addEventListener('abort', () => fullFleetMigrationRun, { once: true })
      break
    }
    default:
      throw new Error(`Unknown migration strategy: ${dbMigrationStrategy}`)
  }
}

export async function tenantHasMigrations(tenantId: string, migration: keyof typeof DBMigration) {
  let migrationVersion: keyof typeof DBMigration | undefined
  if (isMultitenant) {
    const tenant = await getTenantConfig(tenantId)
    migrationVersion = await (getCachedTenantMigration(tenant) ??
      (needsObservation(tenant)
        ? observeTenantMigration(tenantId, tenant)
        : tenant.migrationVersion))
  } else {
    migrationVersion = await lastLocalMigrationName()
  }

  return Boolean(migrationVersion && DBMigration[migrationVersion] >= DBMigration[migration])
}

/**
 * List all tenants that needs to have the migrations run
 */
export async function* listTenantsToMigrate(signal: AbortSignal) {
  let lastCursor = 0
  const knownMigrationVersions = Object.keys(DBMigration)

  while (!signal.aborted) {
    const migrationVersion = await lastLocalMigrationName()

    const data = await tenantConfigStorePg.listTenantsToMigrateBatch(
      migrationVersion,
      lastCursor,
      [TenantMigrationStatus.FAILED, TenantMigrationStatus.FAILED_STALE],
      200,
      knownMigrationVersions,
      signal
    )

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

  while (!signal.aborted) {
    const afterMigrations = Object.keys(DBMigration).filter((migrationName) => {
      return DBMigration[migrationName as keyof typeof DBMigration] > DBMigration[migration]
    })

    const data = await tenantConfigStorePg.listTenantsToResetMigrationsBatch(
      afterMigrations,
      lastCursor,
      200,
      signal
    )

    if (data.length === 0) {
      break
    }

    lastCursor = data[data.length - 1].cursor_id
    yield data.map((tenant) => tenant.id)
  }
}

/** Record the observed ledger only while the captured version and database URL are still current. */
export async function completeTenantMigrations(
  tenantId: string,
  options: CompleteMigrationsOptions
) {
  // This binary cannot certify recovery of a migration it does not know.
  if (!isDBMigrationName(options.migration)) {
    return 0
  }

  return tenantConfigStorePg.completeMigrations(tenantId, options)
}

export async function failTenantMigrations(tenantId: string, options: FailMigrationsOptions) {
  return tenantConfigStorePg.failMigrations(tenantId, options)
}

/**
 * Determine if a tenant has the migrations up to date
 * @param tenantId
 */
export async function areMigrationsUpToDate(tenantId: string) {
  const latestMigrationVersion = await lastLocalMigrationName()
  const tenant = await getTenantConfig(tenantId)

  if (
    isDBMigrationName(tenant.observedMigrationName) &&
    DBMigration[tenant.observedMigrationName] < DBMigration[latestMigrationVersion]
  ) {
    return false
  }

  if (needsObservation(tenant)) {
    // An ahead head skips execution only while its ledger observation is fresh.
    const observed = await observeTenantMigration(tenantId, tenant).catch(() => undefined)
    if (isUnrecognizedMigration(tenant.observedMigrationName)) return true
    if (isUnrecognizedMigration(tenant.migrationVersion)) {
      return observed === highestLocalMigrationName()
    }
    if (observed && DBMigration[observed] < DBMigration[latestMigrationVersion]) return false
  }

  return (
    tenant.migrationVersion &&
    DBMigration[latestMigrationVersion] <= DBMigration[tenant.migrationVersion] &&
    tenant.migrationStatus === TenantMigrationStatus.COMPLETED
  )
}

export async function obtainLockOnMultitenantDB<T>(
  fn: (tnx: DatabaseTransaction) => Promise<T>,
  options?: { sbReqId?: string }
) {
  const trx = await multitenantPgExecutor.beginTransaction()
  try {
    const result = await trx.query<{ locked: boolean }>({
      text: `SELECT pg_try_advisory_xact_lock($1) AS locked;`,
      values: [-8575985245963000605],
    })
    const lockAcquired = result.rows.shift()?.locked || false

    if (!lockAcquired) {
      try {
        await trx.rollback()
      } catch (rollbackError) {
        logSchema.warning(logger, '[Migrations] Failed to rollback transaction', {
          type: 'migrations',
          sbReqId: options?.sbReqId,
          error: rollbackError,
          metadata: JSON.stringify({
            reason: 'lock not acquired',
          }),
        })
      }
      return
    }

    logSchema.info(logger, '[Migrations] Instance acquired the lock', {
      type: 'migrations',
      sbReqId: options?.sbReqId,
    })

    const fnResult = await fn(trx)
    await trx.commit()
    return fnResult
  } catch (e) {
    try {
      await trx.rollback()
    } catch (rollbackError) {
      logSchema.warning(logger, '[Migrations] Failed to rollback transaction', {
        type: 'migrations',
        sbReqId: options?.sbReqId,
        error: rollbackError,
        metadata: JSON.stringify({ originalError: String(e) }),
      })
    }
    throw e
  }
}

export async function resetMigrationsOnTenants(options: {
  till: keyof typeof DBMigration
  markCompletedTillMigration?: keyof typeof DBMigration
  signal: AbortSignal
  sbReqId?: string
}) {
  await obtainLockOnMultitenantDB(
    async () => {
      logSchema.info(logger, '[Migrations] Listing all tenants', {
        type: 'migrations',
        sbReqId: options.sbReqId,
      })

      const tenants = listTenantsToResetMigrations(options.till, options.signal)

      for await (const tenantBatch of tenants) {
        await ResetMigrationsOnTenant.batchSend(
          tenantBatch.map((tenant) => {
            return new ResetMigrationsOnTenant({
              tenantId: tenant,
              untilMigration: options.till,
              markCompletedTillMigration: options.markCompletedTillMigration,
              sbReqId: options.sbReqId,
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
        sbReqId: options.sbReqId,
      })
    },
    { sbReqId: options.sbReqId }
  )
}

/**
 * Runs migrations for all tenants
 * only one instance at the time is allowed to run
 */
export async function runMigrationsOnAllTenants(options: {
  signal: AbortSignal
  sbReqId?: string
}) {
  if (!pgQueueEnable) {
    return
  }
  await obtainLockOnMultitenantDB(
    async () => {
      logSchema.info(logger, '[Migrations] Listing all tenants', {
        type: 'migrations',
        sbReqId: options.sbReqId,
      })
      const tenants = listTenantsToMigrate(options.signal)
      for await (const tenantBatch of tenants) {
        await RunMigrationsOnTenants.batchSend(
          tenantBatch.map((tenant) => {
            return new RunMigrationsOnTenants({
              tenantId: tenant,
              sbReqId: options.sbReqId,
              upToMigration: dbMigrationFreezeAt,
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
        sbReqId: options.sbReqId,
      })
    },
    { sbReqId: options.sbReqId }
  )
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
    migrationsTableSchema: 'public',
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

/** Read schema capabilities without applying migrations or certifying control state. */
export async function readTenantMigrationVersion({
  databaseUrl,
  tenantId,
}: Pick<MigrateOnTenantOptions, 'databaseUrl' | 'tenantId'>): Promise<string> {
  const client = await connect({
    connectionString: databaseUrl,
    tenantId,
    ssl: getSslSettings({ connectionString: databaseUrl, databaseSSLRootCert }),
    connectionTimeoutMillis: databaseConnectionTimeout,
    query_timeout: databaseStatementTimeout,
  })
  try {
    const result = await client.query<{ name: string }>(
      'SELECT name FROM storage.migrations ORDER BY id DESC LIMIT 1'
    )
    if (!result.rows[0]?.name) {
      throw ERRORS.InternalError(undefined, 'Tenant migration ledger is empty')
    }
    return result.rows[0].name
  } finally {
    await client.end()
  }
}

/**
 * Runs migrations on a specific tenant by providing its database DSN
 * @param databaseUrl
 * @param tenantId
 * @param waitForLock
 * @param upToMigration
 * @returns the highest ledger name after the run
 */
export async function runMigrationsOnTenant({
  databaseUrl,
  tenantId,
  waitForLock,
  upToMigration,
}: MigrateOnTenantOptions): Promise<string> {
  // default waitForLock to true
  if (typeof waitForLock === 'undefined') {
    waitForLock = true
  }

  const migrationVersion = await connectAndMigrate({
    databaseUrl,
    migrationsDirectory: './migrations/tenant',
    migrationsTableSchema: 'storage',
    ssl: getSslSettings({ connectionString: databaseUrl, databaseSSLRootCert }),
    shouldCreateStorageSchema: true,
    tenantId,
    waitForLock,
    upToMigration,
  })

  // pgvector mode: run the vector_store migrations after the standard tenant
  // migrations. Branching:
  //   • Multi-tenant: vectors live in each tenant's DB (per-tenant schema
  //     isolation via TenantConnection), so we migrate into the same URL.
  //   • Single-tenant: vectors live in a dedicated `storage_vectors` database
  //     on the server pointed at by VECTOR_DATABASE_URL. The migration runner
  //     CREATE DATABASE's it and then connects via the derived URL. Set
  //     VECTOR_DATABASE_CREATE=false to run vector migrations in the configured
  //     database instead, for gateways that do not support CREATE DATABASE.
  if (vectorBucketProvider === 'pgvector' && vectorStoreMigrationsEnabled) {
    if (isMultitenant) {
      await runVectorStoreMigrations({ databaseUrl, waitForLock })
    } else if (vectorDatabaseURL) {
      await runVectorStoreMigrations({
        databaseUrl: vectorDatabaseURL,
        createDatabase: vectorDatabaseCreate,
        waitForLock,
      })
    }
  }

  return migrationVersion
}

// Re-exported from a leaf module so request-path code (http/plugins/vector.ts)
// can import these without pulling in the migration runner.
export { deriveVectorDatabaseUrl, VECTOR_DATABASE_NAME } from '../vector-store-url'

/**
 * Creates the `storage_vectors` Postgres database on the same server as
 * `maintenanceUrl` if it does not already exist. Connects to the URL as-is —
 * the caller is expected to point it at an existing DB on the target server
 * (e.g. `postgres`, the default maintenance DB).
 */
async function ensureVectorDatabaseExists(
  maintenanceUrl: string,
  ssl: ClientConfig['ssl']
): Promise<void> {
  let defaultAccessMethod = ''
  const client = await connect({
    connectionString: maintenanceUrl,
    ssl,
  })
  try {
    defaultAccessMethod = await getDefaultAccessMethod(client)
    const exists = await client.query(`SELECT 1 FROM pg_database WHERE datname = $1`, [
      VECTOR_DATABASE_NAME,
    ])
    if (exists.rows.length === 0) {
      // CREATE DATABASE doesn't accept parameter binding; the name is a
      // hard-coded constant we control, so direct interpolation is safe.
      await client.query(`CREATE DATABASE "${VECTOR_DATABASE_NAME}"`)
      logSchema.info(logger, `[Migrations] Created database ${VECTOR_DATABASE_NAME}`, {
        type: 'migrations',
      })
    }
  } finally {
    await client.end()
  }

  await configureVectorDatabaseAccessMethod({
    databaseUrl: deriveVectorDatabaseUrl(maintenanceUrl),
    defaultAccessMethod,
    ssl,
  })
}

async function configureVectorDatabaseAccessMethod({
  databaseUrl,
  defaultAccessMethod,
  ssl,
}: {
  databaseUrl: string
  defaultAccessMethod: string
  ssl: ClientConfig['ssl']
}): Promise<void> {
  if (defaultAccessMethod !== 'orioledb') {
    return
  }

  const client = await connect({
    connectionString: databaseUrl,
    ssl,
  })
  try {
    await client.query('CREATE EXTENSION IF NOT EXISTS orioledb')
    await client.query(
      `ALTER DATABASE "${VECTOR_DATABASE_NAME}" SET default_table_access_method = 'orioledb'`
    )
    logSchema.info(logger, `[Migrations] Configured database ${VECTOR_DATABASE_NAME} for Oriole`, {
      type: 'migrations',
    })
  } finally {
    await client.end()
  }
}

/**
 * Runs vector-store migrations against a Postgres database. The migrations live
 * in ./migrations/vector_store and are tracked in storage_vectors.migrations,
 * isolated from the standard tenant migrations (storage.migrations) by schema.
 *
 * Only invoked when VECTOR_BUCKET_PROVIDER=pgvector and
 * VECTOR_STORE_MIGRATIONS_ENABLED=true (gated by the caller).
 *
 * @param databaseUrl     when `createDatabase=true` (single-tenant), this is the
 *                        maintenance URL on the target Postgres server; the
 *                        runner will CREATE DATABASE `storage_vectors` against
 *                        it and run migrations on the derived URL. When
 *                        `createDatabase=false` (multi-tenant), this is the
 *                        tenant's own DB URL and migrations run there directly.
 * @param createDatabase  bootstrap the dedicated `storage_vectors` database
 *                        before migrating into it (single-tenant only).
 */
export async function runVectorStoreMigrations({
  databaseUrl,
  createDatabase = false,
  waitForLock = true,
}: {
  databaseUrl: string
  createDatabase?: boolean
  waitForLock?: boolean
}): Promise<void> {
  logSchema.info(logger, '[Migrations] Running vector_store migrations', {
    type: 'migrations',
  })
  const ssl = getSslSettings({ connectionString: databaseUrl, databaseSSLRootCert })

  let migrationsTarget = databaseUrl
  if (createDatabase) {
    await ensureVectorDatabaseExists(databaseUrl, ssl)
    migrationsTarget = deriveVectorDatabaseUrl(databaseUrl)
  }

  await connectAndMigrate({
    databaseUrl: migrationsTarget,
    migrationsDirectory: './migrations/vector_store',
    // Schema-scoped tracking: storage_vectors.migrations lives in its own
    // schema, so it can't collide with the storage.migrations table used by
    // the standard tenant migrations even though both share the default
    // table name. postgres-migrations bundles a `0_create-migrations-table`
    // bootstrap that hardcodes the literal name `migrations`, so we cannot
    // rename it without forking the library.
    migrationsTableSchema: 'storage_vectors',
    ssl,
    // Bootstrap the storage_vectors schema before postgres-migrations tries to
    // create its tracking table inside it.
    shouldCreateStorageSchema: true,
    waitForLock,
  })
}

export async function resetMigration(options: {
  tenantId?: string
  untilMigration: keyof typeof DBMigration
  markCompletedTillMigration?: keyof typeof DBMigration
  // The idempotency runner manages non-replayable migrations itself.
  skipResetFloorValidation?: boolean
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
      await pgClient.query(`SET search_path TO ${searchPath.join(',')}`)

      const migrationsRowsResult = await pgClient.query(`SELECT * from migrations`)
      const currentTenantMigrations = migrationsRowsResult.rows as { id: number; name: string }[]

      if (!currentTenantMigrations.length) {
        return false
      }

      const currentLastMigration = currentTenantMigrations[currentTenantMigrations.length - 1]
      const localMigration = DBMigration[options.untilMigration]
      const markCompletedMigration = options.markCompletedTillMigration
        ? DBMigration[options.markCompletedTillMigration]
        : undefined
      const completedThroughMigration = markCompletedMigration ?? localMigration
      let unsafeReset: (typeof MIGRATION_RESET_FLOORS)[number] | undefined

      if (!options.skipResetFloorValidation) {
        for (const resetFloor of MIGRATION_RESET_FLOORS) {
          if (completedThroughMigration >= DBMigration[resetFloor.migration]) {
            continue
          }

          const activatedInHistory = currentTenantMigrations.some(
            (currentMigration) => currentMigration.id >= DBMigration[resetFloor.activatedBy]
          )
          const activatedInSchema = activatedInHistory
            ? false
            : (await pgClient.query(resetFloor.schemaCheck)).rows[0]?.reset_floor_active === true

          if (activatedInHistory || activatedInSchema) {
            unsafeReset = resetFloor
            break
          }
        }
      }

      if (unsafeReset) {
        throw new Error(unsafeReset.errorMessage)
      }

      // This tenant migration is already at the desired migration
      if (
        currentLastMigration.id === localMigration &&
        (markCompletedMigration === undefined || currentLastMigration.id >= markCompletedMigration)
      ) {
        return false
      }

      // This tenant migration is behind of the desired migration
      if (currentLastMigration.id < localMigration) {
        return false
      }

      await pgClient.query(`BEGIN`)

      try {
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
            const localFileMigrations = await localMigrationFiles()

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

        if (options.tenantId) {
          await tenantConfigStorePg.update(options.tenantId, {
            migrations_version: latestRunMigration,
            migrations_status: TenantMigrationStatus.COMPLETED,
          })
        }

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
  connectionTimeoutMillis?: number
  query_timeout?: number
}) {
  const { ssl, tenantId, connectionString } = options

  const dbConfig: ClientConfig = {
    connectionString,
    connectionTimeoutMillis: options.connectionTimeoutMillis ?? 60_000,
    query_timeout: options.query_timeout,
    // Migrations run trusted dynamic PL/pgSQL that a multigres gateway
    // rejects by default; only this connection opts into multigres
    // per-connection bypass, instead of the whole gateway being unsafe.
    // No-op against a plain postgres backend (accepted as a placeholder GUC).
    // direct_connection is also accepted, but multigres.unsafe_connection is
    // more explicit and less likely to be confused with a real connection mode.
    options: `-c search_path=${searchPath} -c multigres.unsafe_connection=on${
      options.query_timeout ? ` -c statement_timeout=${options.query_timeout}` : ''
    }`,
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
  migrationsTableSchema?: string
  ssl?: ClientConfig['ssl']
  shouldCreateStorageSchema?: boolean
  tenantId?: string
  waitForLock?: boolean
  upToMigration?: keyof typeof DBMigration
}): Promise<string> {
  const { shouldCreateStorageSchema, migrationsDirectory, ssl, databaseUrl, waitForLock } = options

  const dbConfig: ClientConfig = {
    connectionString: databaseUrl,
    connectionTimeoutMillis: 60_000,
    options: `-c search_path=${searchPath}`,
    ssl,
  }

  const client = await connect(dbConfig)

  try {
    await client.query(`SET statement_timeout TO '12h'`)
    return await migrate({
      client,
      migrationsDirectory,
      migrationsTableSchema: options.migrationsTableSchema,
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
  migrationsTableSchema?: string
  waitForLock: boolean
  shouldCreateStorageSchema?: boolean
  upToMigration?: keyof typeof DBMigration
}

/**
 * Run migrations and observe the ledger head under the same advisory lock.
 * @param dbConfig
 * @param migrationsDirectory
 * @param waitForLock
 * @param shouldCreateStorageSchema
 */
export async function migrate({
  client,
  migrationsDirectory,
  migrationsTableSchema,
  waitForLock,
  shouldCreateStorageSchema,
  upToMigration,
}: MigrateOptions): Promise<string> {
  const accessMethod = await getDefaultAccessMethod(client)
  const applyMigrations = runMigrations({
    migrationsDirectory,
    migrationsTableSchema,
    shouldCreateStorageSchema,
    upToMigration,
    // Remove concurrent index creation if we're using oriole db as it does not support it currently
    transformers: accessMethod === 'orioledb' ? [new DisableConcurrentIndexTransformer()] : [],
  })
  return withAdvisoryLock(waitForLock, async (lockedClient) => {
    await applyMigrations(lockedClient)
    // A reset can rewind the ledger as soon as this lock is released.
    const result = await lockedClient.query('SELECT name FROM migrations ORDER BY id DESC LIMIT 1')
    const migrationVersion: unknown = result.rows[0]?.name
    if (typeof migrationVersion !== 'string' || !migrationVersion) {
      throw ERRORS.InternalError(undefined, 'Migration run returned no ledger position')
    }
    return migrationVersion
  })(client)
}

interface RunMigrationOptions {
  migrationsDirectory: string
  migrationsTableSchema?: string
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
  migrationsTableSchema,
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

      // If migrations are tracked in a non-default schema (e.g. storage_vectors),
      // prepend it to search_path so the postgres-migrations library's bundled
      // bootstrap (which references `migrations` unqualified) resolves to the
      // right table — otherwise we'd collide with storage.migrations.
      const effectiveSearchPath =
        migrationsTableSchema && !searchPath.includes(migrationsTableSchema)
          ? [migrationsTableSchema, ...searchPath]
          : searchPath
      await client.query(`SET search_path TO ${effectiveSearchPath.join(',')}`)

      let appliedMigrations: Migration[] = []
      if (
        await doesTableExist({
          client,
          schemaName: migrationsTableSchema,
          tableName: migrationTableName,
        })
      ) {
        const selectQueryCurrentMigration = SQL`SELECT * FROM `
          .append(migrationTableName)
          .append(SQL` WHERE id <= ${lastMigrationId} ORDER BY id`)

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
        const targetSchema = migrationsTableSchema ?? 'storage'
        const schemaExists = await doesSchemaExists(client, targetSchema)
        if (!schemaExists) {
          await client.query(`CREATE SCHEMA IF NOT EXISTS ${targetSchema}`)
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

      const icebergShardVar = `{${icebergShards.map((s) => `"${s}"`).join(',')}}`
      const icebergDefaultShard = icebergShards.length > 0 ? icebergShards[0] : ''

      if (migrationsToRun.length > 0) {
        // set_config requires literal values, not bound parameters.
        const lit = (v: string | boolean) => `'${String(v).replace(/'/g, "''")}'`
        await client.query(`SELECT
          set_config('storage.install_roles', ${lit(dbInstallRoles)}, false),
          set_config('storage.multitenant', ${lit(isMultitenant ? 'true' : 'false')}, false),
          set_config('storage.anon_role', ${lit(dbAnonRole)}, false),
          set_config('storage.authenticated_role', ${lit(dbAuthenticatedRole)}, false),
          set_config('storage.service_role', ${lit(dbServiceRole)}, false),
          set_config('storage.super_user', ${lit(dbSuperUser)}, false),
          set_config('storage.iceberg_default_shard', ${lit(icebergDefaultShard)}, false),
          set_config('storage.iceberg_shards', ${lit(icebergShardVar)}, false),
          set_config('storage.vector_bucket_provider', ${lit(vectorBucketProvider)}, false);
        `)
      }

      for (const migration of migrationsToRun) {
        try {
          const ignore = migration.sql.includes('-- postgres-migrations ignore')
          let runnableMigration = ignore
            ? {
                ...migration,
                sql: 'SELECT 1;',
                contents: 'SELECT 1;',
              }
            : migration

          // Generate version-dependent SQL first, then run it at the top level so
          // concurrent index creation and invalid-index repair remain supported.
          if (runnableMigration.sql.includes('-- storage-migrations generate-sql')) {
            const { rows } = await client.query(runnableMigration.sql)
            const sql: unknown = rows[0]?.sql
            if (rows.length !== 1 || typeof sql !== 'string' || sql.trim().length === 0) {
              throw new Error('Generated migration must return exactly one non-empty sql string')
            }
            runnableMigration = { ...runnableMigration, sql }
          }

          const transformedMigration = runMigrationTransformers(runnableMigration, transformers)
          const repairedIndexes = await repairInvalidConcurrentIndexes(client, transformedMigration)

          if (repairedIndexes.length > 0) {
            logSchema.warning(logger, '[Migrations] Removed invalid indexes before retry', {
              type: 'migrations',
              metadata: JSON.stringify({
                migrationId: migration.id,
                migrationName: migration.name,
                indexes: repairedIndexes,
              }),
            })
          }

          await runMigration(migrationTableName, client)(transformedMigration)
        } catch (e) {
          throw ERRORS.DatabaseError(
            `Migration failed. Reason: ${(e as Error).message}`,
            e as MigrationError
          ).withMetadata({
            currentMigrations: appliedMigrations.map((migration) => ({
              id: migration.id,
              name: migration.name,
              hash: migration.hash,
            })),
            migrationsToRun: migrationsToRun.map((migration) => ({
              id: migration.id,
              name: migration.name,
              hash: migration.hash,
            })),
            migrationId: migration.id,
            migrationName: migration.name,
            migrationHash: migration.hash,
          })
        }
      }
    } catch (e) {
      if (e instanceof MigrationError) {
        throw new MigrationError(`Migration failed. Reason: ${(e as Error).message}`, {
          cause: e,
        })
      }

      throw e
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
async function doesTableExist({
  client,
  schemaName,
  tableName,
}: {
  client: BasicPgClient
  schemaName?: string
  tableName: string
}) {
  const result = await client.query(
    SQL`SELECT EXISTS (
  SELECT 1
  FROM   pg_catalog.pg_class c
  JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE  c.relname = ${tableName}
  AND    c.relkind = 'r'
`
      .append(schemaName ? SQL`AND n.nspname = ${schemaName}` : '')
      .append(`);`)
  )

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

  return result.rows.length > 0 && result.rows[0].exists
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
      let acquired = false
      let tries = 1

      const timeout = 3000
      const start = Date.now()

      while (!acquired) {
        const elapsed = Date.now() - start
        if (elapsed > timeout) {
          throw ERRORS.LockTimeout()
        }

        const lockResult = await client.query('SELECT pg_try_advisory_lock(-8525285245963000605);')
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

      return await f(client)
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

      newMigrations.forEach((migration) => {
        console.log(`Migration applied: ${migration.id} - ${migration.name}`)
      })

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
