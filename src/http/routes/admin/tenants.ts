import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import apiKey from '../../plugins/apikey'
import { decrypt, encrypt } from '@internal/auth'
import {
  deleteTenantConfig,
  TenantMigrationStatus,
  multitenantKnex,
  lastMigrationName,
  runMigrationsOnTenant,
  progressiveMigrations,
} from '@internal/database'
import { dbSuperUser, storage } from '../../plugins'

const patchSchema = {
  body: {
    type: 'object',
    properties: {
      anonKey: { type: 'string' },
      databaseUrl: { type: 'string' },
      databasePoolUrl: { type: 'string', nullable: true },
      maxConnections: { type: 'number' },
      fileSizeLimit: { type: 'number' },
      jwtSecret: { type: 'string' },
      jwks: { type: 'object', nullable: true },
      serviceKey: { type: 'string' },
      tracingMode: { type: 'string' },
      features: {
        type: 'object',
        properties: {
          imageTransformation: {
            type: 'object',
            properties: {
              enabled: { type: 'boolean' },
              maxResolution: { type: 'number', nullable: true },
            },
          },
          s3Protocol: {
            type: 'object',
            properties: {
              enabled: { type: 'boolean' },
            },
          },
        },
      },
    },
  },
  optional: ['tracingMode', 'maxResolution'],
} as const

const schema = {
  body: {
    ...patchSchema.body,
    required: ['anonKey', 'databaseUrl', 'jwtSecret', 'serviceKey'],
  },
} as const

interface tenantPatchRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof patchSchema.body>
  Params: {
    tenantId: string
  }
}

interface tenantRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof schema.body>
  Params: {
    tenantId: string
  }
}

interface tenantDBInterface {
  id: string
  anon_key: string
  database_url: string
  database_pool_url?: string
  max_connections?: number
  jwt_secret: string
  jwks: {
    keys?: any[]
  } | null
  service_key: string
  file_size_limit?: number
  feature_s3_protocol?: boolean
  feature_image_transformation?: boolean
  image_transformation_max_resolution?: number
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.get('/', async () => {
    const tenants = await multitenantKnex('tenants').select()
    return tenants.map(
      ({
        id,
        anon_key,
        database_url,
        database_pool_url,
        max_connections,
        file_size_limit,
        jwt_secret,
        jwks,
        service_key,
        feature_image_transformation,
        feature_s3_protocol,
        image_transformation_max_resolution,
        migrations_version,
        migrations_status,
        tracing_mode,
      }) => ({
        id,
        anonKey: decrypt(anon_key),
        databaseUrl: decrypt(database_url),
        databasePoolUrl: database_pool_url ? decrypt(database_pool_url) : undefined,
        maxConnections: max_connections ? Number(max_connections) : undefined,
        fileSizeLimit: Number(file_size_limit),
        jwtSecret: decrypt(jwt_secret),
        jwks,
        serviceKey: decrypt(service_key),
        migrationVersion: migrations_version,
        migrationStatus: migrations_status,
        tracingMode: tracing_mode,
        features: {
          imageTransformation: {
            enabled: feature_image_transformation,
            maxResolution: image_transformation_max_resolution,
          },
          s3Protocol: {
            enabled: feature_s3_protocol,
          },
        },
      })
    )
  })

  fastify.get<tenantRequestInterface>('/:tenantId', async (request, reply) => {
    const tenant = await multitenantKnex('tenants').first().where('id', request.params.tenantId)
    if (!tenant) {
      reply.code(404).send()
    } else {
      const {
        anon_key,
        database_url,
        database_pool_url,
        max_connections,
        file_size_limit,
        jwt_secret,
        jwks,
        service_key,
        feature_s3_protocol,
        feature_image_transformation,
        image_transformation_max_resolution,
        migrations_version,
        migrations_status,
        tracing_mode,
      } = tenant

      return {
        anonKey: decrypt(anon_key),
        databaseUrl: decrypt(database_url),
        databasePoolUrl:
          typeof database_pool_url === null
            ? null
            : database_pool_url
            ? decrypt(database_pool_url)
            : undefined,
        maxConnections: max_connections ? Number(max_connections) : undefined,
        fileSizeLimit: Number(file_size_limit),
        jwtSecret: decrypt(jwt_secret),
        jwks,
        serviceKey: decrypt(service_key),
        features: {
          imageTransformation: {
            enabled: feature_image_transformation,
            maxResolution: image_transformation_max_resolution,
          },
          s3Protocol: {
            enabled: feature_s3_protocol,
          },
        },
        migrationVersion: migrations_version,
        migrationStatus: migrations_status,
        tracingMode: tracing_mode,
      }
    }
  })

  fastify.post<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    const { tenantId } = request.params
    const {
      anonKey,
      databaseUrl,
      fileSizeLimit,
      jwtSecret,
      jwks,
      serviceKey,
      features,
      databasePoolUrl,
      maxConnections,
      tracingMode,
    } = request.body

    await multitenantKnex('tenants').insert({
      id: tenantId,
      anon_key: encrypt(anonKey),
      database_url: encrypt(databaseUrl),
      database_pool_url: databasePoolUrl ? encrypt(databasePoolUrl) : undefined,
      max_connections: maxConnections ? Number(maxConnections) : undefined,
      file_size_limit: fileSizeLimit,
      jwt_secret: encrypt(jwtSecret),
      jwks,
      service_key: encrypt(serviceKey),
      feature_image_transformation: features?.imageTransformation?.enabled ?? false,
      feature_s3_protocol: features?.s3Protocol?.enabled ?? true,
      migrations_version: null,
      migrations_status: null,
      tracing_mode: tracingMode,
    })

    try {
      await runMigrationsOnTenant(databaseUrl, tenantId)
      await multitenantKnex('tenants')
        .where('id', tenantId)
        .update({
          migrations_version: await lastMigrationName(),
          migrations_status: TenantMigrationStatus.COMPLETED,
        })
    } catch (e) {
      progressiveMigrations.addTenant(tenantId)
    }

    reply.code(201).send()
  })

  fastify.patch<tenantPatchRequestInterface>(
    '/:tenantId',
    { schema: patchSchema },
    async (request, reply) => {
      const {
        anonKey,
        databaseUrl,
        fileSizeLimit,
        jwtSecret,
        jwks,
        serviceKey,
        features,
        databasePoolUrl,
        maxConnections,
        tracingMode,
      } = request.body
      const { tenantId } = request.params

      await multitenantKnex('tenants')
        .update({
          anon_key: anonKey !== undefined ? encrypt(anonKey) : undefined,
          database_url: databaseUrl !== undefined ? encrypt(databaseUrl) : undefined,
          database_pool_url: databasePoolUrl
            ? encrypt(databasePoolUrl)
            : databasePoolUrl === null
            ? null
            : undefined,
          max_connections: maxConnections ? Number(maxConnections) : undefined,
          file_size_limit: fileSizeLimit,
          jwt_secret: jwtSecret !== undefined ? encrypt(jwtSecret) : undefined,
          jwks,
          service_key: serviceKey !== undefined ? encrypt(serviceKey) : undefined,
          feature_image_transformation: features?.imageTransformation?.enabled,
          feature_s3_protocol: features?.s3Protocol?.enabled,
          image_transformation_max_resolution:
            features?.imageTransformation?.maxResolution === null
              ? null
              : features?.imageTransformation?.maxResolution,
          tracing_mode: tracingMode,
        })
        .where('id', tenantId)

      if (databaseUrl) {
        try {
          await runMigrationsOnTenant(databaseUrl, tenantId)
          await multitenantKnex('tenants')
            .where('id', tenantId)
            .update({
              migrations_version: await lastMigrationName(),
              migrations_status: TenantMigrationStatus.COMPLETED,
            })
        } catch (e) {
          progressiveMigrations.addTenant(tenantId)
        }
      }

      reply.code(204).send()
    }
  )

  fastify.put<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    const {
      anonKey,
      databaseUrl,
      fileSizeLimit,
      jwtSecret,
      jwks,
      serviceKey,
      features,
      databasePoolUrl,
      maxConnections,
      tracingMode,
    } = request.body
    const { tenantId } = request.params

    const tenantInfo: tenantDBInterface & {
      tracing_mode?: string
    } = {
      id: tenantId,
      anon_key: encrypt(anonKey),
      database_url: encrypt(databaseUrl),
      jwt_secret: encrypt(jwtSecret),
      jwks: jwks || null,
      service_key: encrypt(serviceKey),
    }

    if (fileSizeLimit) {
      tenantInfo.file_size_limit = fileSizeLimit
    }

    if (typeof features?.imageTransformation?.enabled !== 'undefined') {
      tenantInfo.feature_image_transformation = features?.imageTransformation?.enabled
    }

    if (typeof features?.imageTransformation?.maxResolution !== 'undefined') {
      tenantInfo.image_transformation_max_resolution = features?.imageTransformation
        ?.image_transformation_max_resolution as number | undefined
    }

    if (typeof features?.s3Protocol?.enabled !== 'undefined') {
      tenantInfo.feature_s3_protocol = features?.s3Protocol?.enabled
    }

    if (databasePoolUrl) {
      tenantInfo.database_pool_url = encrypt(databasePoolUrl)
    }

    if (maxConnections) {
      tenantInfo.max_connections = Number(maxConnections)
    }

    if (tracingMode) {
      tenantInfo.tracing_mode = tracingMode
    }

    await multitenantKnex('tenants').insert(tenantInfo).onConflict('id').merge()

    try {
      await runMigrationsOnTenant(databaseUrl, tenantId)
      await multitenantKnex('tenants')
        .where('id', tenantId)
        .update({
          migrations_version: await lastMigrationName(),
          migrations_status: TenantMigrationStatus.COMPLETED,
        })
    } catch (e) {
      progressiveMigrations.addTenant(tenantId)
    }

    reply.code(204).send()
  })

  fastify.delete<tenantRequestInterface>('/:tenantId', async (request, reply) => {
    await multitenantKnex('tenants').del().where('id', request.params.tenantId)
    deleteTenantConfig(request.params.tenantId)
    reply.code(204).send()
  })

  fastify.get<tenantRequestInterface>('/:tenantId/migrations', async (req, reply) => {
    const migrationsInfo = await multitenantKnex
      .table<{ migrations_version?: string; migrations_status?: string }>('tenants')
      .select('migrations_version', 'migrations_status')
      .where('id', req.params.tenantId)
      .first()

    if (!migrationsInfo) {
      reply.status(404).send({
        error: 'Tenant not found',
      })
      return
    }

    reply.send({
      isLatest: (await lastMigrationName()) === migrationsInfo?.migrations_version,
      migrationsVersion: migrationsInfo?.migrations_version,
      migrationsStatus: migrationsInfo?.migrations_status,
    })
  })

  fastify.post<tenantRequestInterface>('/:tenantId/migrations', async (req, reply) => {
    const tenantId = req.params.tenantId
    const migrationsInfo = await multitenantKnex
      .table<{ databaseUrl: string }>('tenants')
      .select('database_url')
      .where('id', req.params.tenantId)
      .first()

    if (!migrationsInfo) {
      reply.status(404).send({
        error: 'Tenant not found',
      })
      return
    }

    const databaseUrl = decrypt(migrationsInfo.database_url)

    try {
      await runMigrationsOnTenant(databaseUrl, tenantId)
      reply.send({
        migrated: true,
      })
    } catch (e) {
      reply.status(400).send({
        migrated: false,
        error: JSON.stringify(e),
      })
    }
  })

  fastify.register(async (fastify) => {
    fastify.register(dbSuperUser, {
      disableHostCheck: true,
    })
    fastify.register(storage)

    fastify.get<tenantRequestInterface>('/:tenantId/health', async (req, res) => {
      try {
        await req.storage.healthcheck()
        res.send({ healthy: true })
      } catch (e) {
        if (e instanceof Error) {
          req.executionError = e
        }
        res.send({ healthy: false })
      }
    })
  })
}
