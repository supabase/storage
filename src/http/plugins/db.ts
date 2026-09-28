import { createSingleFlightByKey } from '@internal/concurrency'
import {
  deleteTenantConfig,
  getPostgresConnection,
  getServiceKeyUser,
  getTenantConfig,
  type TenantConnection,
  TenantMigrationStatus,
} from '@internal/database'
import {
  areMigrationsUpToDate,
  completeTenantMigrations,
  DBMigration,
  highestLocalMigrationName,
  isDBMigrationName,
  isUnrecognizedMigration,
  lastLocalMigrationName,
  progressiveMigrations,
  runMigrationsOnTenant,
} from '@internal/database/migrations'
import { ERRORS } from '@internal/errors'
import type { FastifyInstance } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import { getConfig, MultitenantMigrationStrategy } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    db: TenantConnection
    latestMigration?: keyof typeof DBMigration
  }
}

const { databaseEnableQueryCancellation, dbMigrationStrategy, isMultitenant, dbMigrationFreezeAt } =
  getConfig()

const migrationSingleFlight = createSingleFlightByKey<keyof typeof DBMigration>()

function resolveLatestMigration(
  localLatest: keyof typeof DBMigration,
  applied: keyof typeof DBMigration | undefined
): keyof typeof DBMigration {
  if (isUnrecognizedMigration(applied)) {
    return highestLocalMigrationName()
  }
  if (applied && DBMigration[applied] > DBMigration[localLatest]) {
    return applied
  }
  return localLatest
}

export const db = fastifyPlugin(
  async function db(fastify) {
    fastify.register(migrations)

    fastify.decorateRequest('db')

    fastify.addHook('preHandler', async (request) => {
      const adminUser = await getServiceKeyUser(request.tenantId)
      const userPayload = request.jwtPayload

      if (!userPayload) {
        throw ERRORS.AccessDenied('JWT payload is missing')
      }

      request.db = await getPostgresConnection({
        user: {
          payload: userPayload,
          jwt: request.jwt,
        },
        superUser: adminUser,
        tenantId: request.tenantId,
        host: request.headers['x-forwarded-host'] as string,
        headers: request.headers,
        path: request.url,
        method: request.method,
        operation: () => request.operation,
      })

      // Connect abort signal to DB connection for query cancellation
      if (databaseEnableQueryCancellation && request.signals) {
        request.db.setAbortSignal(request.signals.disconnect.signal)
      }
    })

    registerConnectionCleanupHooks(fastify)
  },
  { name: 'db-init' }
)

interface DbSuperUserPluginOptions {
  disableHostCheck?: boolean
}

export const dbSuperUser = fastifyPlugin<DbSuperUserPluginOptions>(
  async function dbSuperUser(fastify, opts) {
    fastify.register(migrations)
    fastify.decorateRequest('db')

    fastify.addHook('preHandler', async (request) => {
      const adminUser = await getServiceKeyUser(request.tenantId)

      request.db = await getPostgresConnection({
        user: adminUser,
        superUser: adminUser,
        tenantId: request.tenantId,
        host: request.headers['x-forwarded-host'] as string,
        path: request.url,
        method: request.method,
        headers: request.headers,
        disableHostCheck: opts.disableHostCheck,
        operation: () => request.operation,
      })

      // Connect abort signal to DB connection for query cancellation
      if (databaseEnableQueryCancellation && request.signals) {
        request.db.setAbortSignal(request.signals.disconnect.signal)
      }
    })

    registerConnectionCleanupHooks(fastify)
  },
  { name: 'db-superuser-init' }
)

function registerConnectionCleanupHooks(fastify: FastifyInstance) {
  fastify.addHook('onSend', (request, _reply, payload, done) => {
    request.db?.dispose()
    done(null, payload)
  })

  fastify.addHook('onTimeout', (request, _reply, done) => {
    request.db?.dispose()
    done()
  })

  fastify.addHook('onRequestAbort', (request, done) => {
    request.db?.dispose()
    done()
  })
}

/**
 * Handle database migration for multitenant applications when a request is made
 */
export const migrations = fastifyPlugin(
  async function migrations(fastify) {
    fastify.addHook('preHandler', async (req) => {
      if (isMultitenant) {
        const { migrationVersion } = await getTenantConfig(req.tenantId)
        // Start from the recorded position, clamping unrecognized names to the
        // highest migration this binary knows.
        // The following hook repairs missing or incomplete snapshots before use.
        req.latestMigration = isUnrecognizedMigration(migrationVersion)
          ? highestLocalMigrationName()
          : migrationVersion
        return
      }

      req.latestMigration = await lastLocalMigrationName()
    })

    fastify.addHook('preHandler', async (request) => {
      if (!isMultitenant) {
        return
      }

      const tenant = await getTenantConfig(request.tenantId)
      const migrateOnRequest = dbMigrationStrategy === MultitenantMigrationStrategy.ON_REQUEST

      // Missing or incomplete snapshots cannot safely select SQL after a
      // restore. Resolve them synchronously before constructing the adapter.
      const needsSnapshotRepair =
        !tenant.migrationVersion || tenant.migrationStatus !== TenantMigrationStatus.COMPLETED
      if (!migrateOnRequest && !needsSnapshotRepair) {
        return
      }

      if (tenant.syncMigrationsDone && tenant.migrationVersion) {
        request.latestMigration = resolveLatestMigration(
          await lastLocalMigrationName(),
          tenant.migrationVersion
        )
        return
      }

      request.latestMigration = await migrationSingleFlight(request.tenantId, async () => {
        const appliedMigration = tenant.migrationVersion
        const expectedMigrationVersion = appliedMigration ?? null
        const localLatest = await lastLocalMigrationName()
        const migrationsUpToDate = await areMigrationsUpToDate(request.tenantId)
        let physicalMigration: string | undefined

        if (!migrationsUpToDate) {
          physicalMigration = await runMigrationsOnTenant({
            databaseUrl: tenant.databaseUrl,
            tenantId: request.tenantId,
            upToMigration: dbMigrationFreezeAt,
            returnMigrationVersion: true,
          })
          deleteTenantConfig(request.tenantId)
          if (!physicalMigration) {
            throw ERRORS.InternalError(undefined, 'Migration run returned no ledger position')
          }
        }

        let refreshedTenant = await getTenantConfig(request.tenantId)
        const physicalMigrationIsUnknown = isUnrecognizedMigration(physicalMigration)
        const knownPhysicalMigration = isDBMigrationName(physicalMigration)
          ? physicalMigration
          : undefined
        const preserveRefreshedMigrationState =
          physicalMigrationIsUnknown ||
          (!knownPhysicalMigration && isUnrecognizedMigration(refreshedTenant.migrationVersion))
        const resolvedMigration = physicalMigrationIsUnknown
          ? highestLocalMigrationName()
          : (knownPhysicalMigration ??
            resolveLatestMigration(
              resolveLatestMigration(localLatest, appliedMigration),
              refreshedTenant.migrationVersion
            ))

        if (knownPhysicalMigration) {
          const updated = await completeTenantMigrations(request.tenantId, {
            expectedMigrationVersion,
            migration: knownPhysicalMigration,
          })
          if (updated === 0) {
            deleteTenantConfig(request.tenantId)
            refreshedTenant = await getTenantConfig(request.tenantId)
            refreshedTenant.syncMigrationsDone = true
            return resolveLatestMigration(knownPhysicalMigration, refreshedTenant.migrationVersion)
          }
        }

        if (!preserveRefreshedMigrationState) {
          refreshedTenant.migrationVersion = resolvedMigration
          refreshedTenant.migrationStatus = TenantMigrationStatus.COMPLETED
        }
        refreshedTenant.syncMigrationsDone = true

        return resolvedMigration
      })
    })

    if (dbMigrationStrategy === MultitenantMigrationStrategy.PROGRESSIVE) {
      fastify.addHook('preHandler', async (request) => {
        if (!isMultitenant) {
          return
        }

        const tenant = await getTenantConfig(request.tenantId)
        if (tenant.syncMigrationsDone) {
          return
        }

        // migrations are up to date
        if (await areMigrationsUpToDate(request.tenantId)) {
          tenant.syncMigrationsDone = true
          return
        }

        progressiveMigrations.addTenant(request.tenantId)
      })
    }
  },
  { name: 'db-migrations' }
)
