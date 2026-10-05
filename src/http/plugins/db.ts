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
  cacheTenantMigration,
  completeTenantMigrations,
  DBMigration,
  getCachedTenantMigration,
  highestLocalMigrationName,
  isDBMigrationName,
  isUnrecognizedMigration,
  lastLocalMigrationName,
  needsObservation,
  observeTenantMigration,
  progressiveMigrations,
  runMigrationsOnTenant,
} from '@internal/database/migrations'
import { ERRORS, ErrorCode, StorageBackendError } from '@internal/errors'
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
  applied: string | undefined
): keyof typeof DBMigration {
  if (isDBMigrationName(applied)) {
    return DBMigration[applied] > DBMigration[localLatest] ? applied : localLatest
  }
  return applied ? highestLocalMigrationName() : localLatest
}

async function getUnchangedTenant(
  tenantId: string,
  tenant: Awaited<ReturnType<typeof getTenantConfig>>
) {
  const current = await getTenantConfig(tenantId)
  if (current.databaseUrlEncrypted !== tenant.databaseUrlEncrypted) {
    throw StorageBackendError.withStatusCode(503, {
      code: ErrorCode.DatabaseError,
      httpStatusCode: 503,
      message: 'The tenant database changed during the request. Please try again later.',
    })
  }
  return current
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
    fastify.addHook('preHandler', async (request) => {
      if (!isMultitenant) {
        request.latestMigration = await lastLocalMigrationName()
        return
      }

      const tenant = await getTenantConfig(request.tenantId)
      const { migrationVersion, migrationStatus } = tenant

      if (dbMigrationStrategy === MultitenantMigrationStrategy.ON_REQUEST) {
        const cached = getCachedTenantMigration(tenant)
        if (tenant.syncMigrationsDone && cached) {
          request.latestMigration = await cached
          return
        }

        request.latestMigration = await migrationSingleFlight(request.tenantId, async () => {
          const localLatest = await lastLocalMigrationName()
          let physicalMigration: string | undefined

          if (!(await areMigrationsUpToDate(request.tenantId))) {
            try {
              physicalMigration = await runMigrationsOnTenant({
                databaseUrl: tenant.databaseUrl,
                tenantId: request.tenantId,
                upToMigration: dbMigrationFreezeAt,
              })
              await completeTenantMigrations(request.tenantId, {
                expectedMigrationVersion: migrationVersion ?? null,
                expectedDatabaseUrl: tenant.databaseUrlEncrypted,
                migration: physicalMigration,
              })
            } finally {
              deleteTenantConfig(request.tenantId)
            }
          }

          const refreshedTenant = await getUnchangedTenant(request.tenantId, tenant)
          // A concurrent reset rewrites the control row, not the DDL already applied.
          let latestMigration = resolveLatestMigration(
            resolveLatestMigration(localLatest, physicalMigration),
            refreshedTenant.migrationVersion
          )
          if (!physicalMigration) {
            const observed = getCachedTenantMigration(refreshedTenant)
            if (observed || needsObservation(refreshedTenant)) {
              latestMigration = await (observed ??
                observeTenantMigration(request.tenantId, refreshedTenant))
            }
          }
          if (physicalMigration && isUnrecognizedMigration(physicalMigration)) {
            cacheTenantMigration(refreshedTenant, physicalMigration)
          } else if (
            isDBMigrationName(refreshedTenant.migrationVersion) &&
            refreshedTenant.migrationStatus === TenantMigrationStatus.COMPLETED &&
            !isUnrecognizedMigration(refreshedTenant.observedMigrationName)
          ) {
            cacheTenantMigration(refreshedTenant, latestMigration)
            refreshedTenant.syncMigrationsDone = true
          }
          return latestMigration
        })
        return
      }

      const incomplete = !migrationVersion || migrationStatus !== TenantMigrationStatus.COMPLETED
      if (needsObservation(tenant)) {
        // Queue incomplete state before reading so an unavailable ledger still gets a retry.
        if (incomplete) progressiveMigrations.addTenant(request.tenantId)
        const observed = await observeTenantMigration(request.tenantId, tenant)
        await getUnchangedTenant(request.tenantId, tenant)
        request.latestMigration = observed
        if (!incomplete && observed !== highestLocalMigrationName()) {
          progressiveMigrations.addTenant(request.tenantId)
        }
        return
      }
      request.latestMigration = await (getCachedTenantMigration(tenant) ?? migrationVersion)

      if (!tenant.syncMigrationsDone) {
        if (await areMigrationsUpToDate(request.tenantId)) {
          tenant.syncMigrationsDone = true
        } else {
          progressiveMigrations.addTenant(request.tenantId)
        }
      }
    })
  },
  { name: 'db-migrations' }
)
