import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import apiKey from '../../plugins/apikey'
import { decrypt, encrypt } from '../../../auth'
import { knex } from '../../../database/multitenant-db'
import { deleteTenantConfig, runMigrations } from '../../../database/tenant'
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
      serviceKey: { type: 'string' },
      features: {
        type: 'object',
        properties: {
          imageTransformation: {
            type: 'object',
            properties: {
              enabled: { type: 'boolean' },
            },
          },
        },
      },
    },
  },
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
  service_key: string
  file_size_limit?: number
  feature_image_transformation?: boolean
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.get('/', async () => {
    const tenants = await knex('tenants').select()
    return tenants.map(
      ({
        id,
        anon_key,
        database_url,
        database_pool_url,
        max_connections,
        file_size_limit,
        jwt_secret,
        service_key,
        feature_image_transformation,
      }) => ({
        id,
        anonKey: decrypt(anon_key),
        databaseUrl: decrypt(database_url),
        databasePoolUrl: database_pool_url ? decrypt(database_pool_url) : undefined,
        maxConnections: max_connections ? Number(max_connections) : undefined,
        fileSizeLimit: Number(file_size_limit),
        jwtSecret: decrypt(jwt_secret),
        serviceKey: decrypt(service_key),
        features: {
          imageTransformation: {
            enabled: feature_image_transformation,
          },
        },
      })
    )
  })

  fastify.get<tenantRequestInterface>('/:tenantId', async (request, reply) => {
    const tenant = await knex('tenants').first().where('id', request.params.tenantId)
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
        service_key,
        feature_image_transformation,
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
        serviceKey: decrypt(service_key),
        features: {
          imageTransformation: {
            enabled: feature_image_transformation,
          },
        },
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
      serviceKey,
      features,
      databasePoolUrl,
      maxConnections,
    } = request.body

    await runMigrations(tenantId, databaseUrl)
    await knex('tenants').insert({
      id: tenantId,
      anon_key: encrypt(anonKey),
      database_url: encrypt(databaseUrl),
      database_pool_url: databasePoolUrl ? encrypt(databasePoolUrl) : undefined,
      max_connections: maxConnections ? Number(maxConnections) : undefined,
      file_size_limit: fileSizeLimit,
      jwt_secret: encrypt(jwtSecret),
      service_key: encrypt(serviceKey),
      feature_image_transformation: features?.imageTransformation?.enabled ?? false,
    })
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
        serviceKey,
        features,
        databasePoolUrl,
        maxConnections,
      } = request.body
      const { tenantId } = request.params
      if (databaseUrl) {
        await runMigrations(tenantId, databaseUrl)
      }
      console.log(databasePoolUrl, databasePoolUrl === null)
      await knex('tenants')
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
          service_key: serviceKey !== undefined ? encrypt(serviceKey) : undefined,
          feature_image_transformation: features?.imageTransformation?.enabled,
        })
        .where('id', tenantId)
      reply.code(204).send()
    }
  )

  fastify.put<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    const {
      anonKey,
      databaseUrl,
      fileSizeLimit,
      jwtSecret,
      serviceKey,
      features,
      databasePoolUrl,
      maxConnections,
    } = request.body
    const { tenantId } = request.params
    await runMigrations(tenantId, databaseUrl)

    const tenantInfo: tenantDBInterface = {
      id: tenantId,
      anon_key: encrypt(anonKey),
      database_url: encrypt(databaseUrl),
      jwt_secret: encrypt(jwtSecret),
      service_key: encrypt(serviceKey),
    }

    if (fileSizeLimit) {
      tenantInfo.file_size_limit = fileSizeLimit
    }

    if (typeof features?.imageTransformation?.enabled !== 'undefined') {
      tenantInfo.feature_image_transformation = features?.imageTransformation?.enabled
    }

    if (databasePoolUrl) {
      tenantInfo.database_pool_url = encrypt(databasePoolUrl)
    }

    if (maxConnections) {
      tenantInfo.max_connections = Number(maxConnections)
    }

    await knex('tenants').insert(tenantInfo).onConflict('id').merge()
    reply.code(204).send()
  })

  fastify.delete<tenantRequestInterface>('/:tenantId', async (request, reply) => {
    await knex('tenants').del().where('id', request.params.tenantId)
    deleteTenantConfig(request.params.tenantId)
    reply.code(204).send()
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
          res.executionError = e
        }
        res.send({ healthy: false })
      }
    })
  })
}
