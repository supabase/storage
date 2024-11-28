import fastifyPlugin from 'fastify-plugin'
import { getConfig, MultitenantMigrationStrategy } from '../../config'
import {
  areMigrationsUpToDate,
  getServiceKeyUser,
  getTenantConfig,
  TenantMigrationStatus,
  updateTenantMigrationsState,
  TenantConnection,
  getPostgresConnection,
  progressiveMigrations,
  runMigrationsOnTenant,
  hasMissingSyncMigration,
  DBMigration,
  lastMigrationName,
} from '@internal/database'
import { verifyJWT } from '@internal/auth'
import { logSchema } from '@internal/monitoring'
import { createMutexByKey } from '@internal/concurrency'

declare module 'fastify' {
  interface FastifyRequest {
    db: TenantConnection
    latestMigration?: keyof typeof DBMigration
  }
}

const { dbMigrationStrategy, isMultitenant } = getConfig()

export const db = fastifyPlugin(
  async function db(fastify) {
    fastify.register(migrations)

    fastify.decorateRequest('db', null)

    fastify.addHook('preHandler', async (request) => {
      const adminUser = await getServiceKeyUser(request.tenantId)
      const userPayload =
        request.jwtPayload ?? (await verifyJWT<{ role?: string }>(request.jwt, adminUser.jwtSecret))

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
        operation: () => request.operation?.type,
      })
    })

    fastify.addHook('onSend', async (request, reply, payload) => {
      if (request.db) {
        request.db.dispose().catch((e) => {
          logSchema.error(request.log, 'Error disposing db connection', {
            type: 'db-connection',
            error: e,
          })
        })
      }
      return payload
    })

    fastify.addHook('onTimeout', async (request) => {
      if (request.db) {
        request.db.dispose().catch((e) => {
          logSchema.error(request.log, 'Error disposing db connection', {
            type: 'db-connection',
            error: e,
          })
        })
      }
    })

    fastify.addHook('onRequestAbort', async (request) => {
      if (request.db) {
        request.db.dispose().catch((e) => {
          logSchema.error(request.log, 'Error disposing db connection', {
            type: 'db-connection',
            error: e,
          })
        })
      }
    })
  },
  { name: 'db-init' }
)

interface DbSuperUserPluginOptions {
  disableHostCheck?: boolean
}

export const dbSuperUser = fastifyPlugin<DbSuperUserPluginOptions>(
  async function dbSuperUser(fastify, opts) {
    fastify.register(migrations)
    fastify.decorateRequest('db', null)

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
        operation: () => request.operation?.type,
      })
    })

    fastify.addHook('onSend', async (request, reply, payload) => {
      if (request.db) {
        request.db.dispose().catch((e) => {
          logSchema.error(request.log, 'Error disposing db connection', {
            type: 'db-connection',
            error: e,
          })
        })
      }

      return payload
    })

    fastify.addHook('onTimeout', async (request) => {
      if (request.db) {
        request.db.dispose().catch((e) => {
          logSchema.error(request.log, 'Error disposing db connection', {
            type: 'db-connection',
            error: e,
          })
        })
      }
    })

    fastify.addHook('onRequestAbort', async (request) => {
      if (request.db) {
        request.db.dispose().catch((e) => {
          logSchema.error(request.log, 'Error disposing db connection', {
            type: 'db-connection',
            error: e,
          })
        })
      }
    })
  },
  { name: 'db-superuser-init' }
)

/**
 * Handle database migration for multitenant applications when a request is made
 */
export const migrations = fastifyPlugin(
  async function migrations(fastify) {
    fastify.addHook('preHandler', async (req) => {
      if (isMultitenant) {
        const { migrationVersion } = await getTenantConfig(req.tenantId)
        req.latestMigration = migrationVersion
        return
      }

      req.latestMigration = await lastMigrationName()
    })

    if (dbMigrationStrategy === MultitenantMigrationStrategy.ON_REQUEST) {
      const migrationsMutex = createMutexByKey()

      fastify.addHook('preHandler', async (request) => {
        // migrations are handled via async migrations
        if (!isMultitenant) {
          return
        }

        const tenant = await getTenantConfig(request.tenantId)
        const migrationsUpToDate = await areMigrationsUpToDate(request.tenantId)

        if (tenant.syncMigrationsDone || migrationsUpToDate) {
          return
        }

        await migrationsMutex(request.tenantId, async () => {
          const tenant = await getTenantConfig(request.tenantId)

          if (tenant.syncMigrationsDone) {
            return
          }

          await runMigrationsOnTenant(tenant.databaseUrl, request.tenantId)
          await updateTenantMigrationsState(request.tenantId)
          tenant.syncMigrationsDone = true
        })
      })
    }

    if (dbMigrationStrategy === MultitenantMigrationStrategy.PROGRESSIVE) {
      const migrationsMutex = createMutexByKey()
      fastify.addHook('preHandler', async (request) => {
        if (!isMultitenant) {
          return
        }

        const tenant = await getTenantConfig(request.tenantId)
        const migrationsUpToDate = await areMigrationsUpToDate(request.tenantId)

        // migrations are up to date
        if (tenant.syncMigrationsDone || migrationsUpToDate) {
          return
        }

        const needsToRunMigrationsNow = await hasMissingSyncMigration(request.tenantId)

        // if the tenant is not marked as stale, add it to the progressive migrations queue
        if (
          !needsToRunMigrationsNow &&
          tenant.migrationStatus !== TenantMigrationStatus.FAILED_STALE
        ) {
          progressiveMigrations.addTenant(request.tenantId)
          return
        }

        // if the tenant is marked as stale or there are pending SYNC migrations,
        // try running the migrations
        await migrationsMutex(request.tenantId, async () => {
          if (tenant.syncMigrationsDone || migrationsUpToDate) {
            return
          }

          await runMigrationsOnTenant(tenant.databaseUrl, request.tenantId, needsToRunMigrationsNow)
            .then(async () => {
              await updateTenantMigrationsState(request.tenantId)
              tenant.syncMigrationsDone = true
            })
            .catch((e) => {
              logSchema.error(
                fastify.log,
                `[Migrations] Error running migrations ${request.tenantId} `,
                {
                  type: 'migrations',
                  error: e,
                  metadata: JSON.stringify({
                    strategy: 'progressive',
                  }),
                }
              )
            })
        }).catch((e) => {
          logSchema.error(
            fastify.log,
            `[Migrations] Error running migrations ${request.tenantId} `,
            {
              type: 'migrations',
              error: e,
              metadata: JSON.stringify({
                strategy: 'progressive',
              }),
            }
          )
        })
      })
    }
  },
  { name: 'db-migrations' }
)
