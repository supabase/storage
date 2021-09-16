import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import apiKey from '../../plugins/apikey'
import { decrypt, encrypt } from '../../utils/crypto'
import { knex } from '../../utils/multitenant-db'
import { deleteTenantConfig } from '../../utils/tenant'

const schema = {
  body: {
    type: 'object',
    required: ['anonKey', 'databaseUrl', 'jwtSecret', 'serviceKey'],
    properties: {
      anonKey: { type: 'string' },
      databaseUrl: { type: 'string' },
      jwtSecret: { type: 'string' },
      serviceKey: { type: 'string' },
    },
  },
} as const

interface tenantRequestInterface extends RequestGenericInterface {
  Body: FromSchema<typeof schema.body>
  Params: {
    tenantId: string
  }
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.get('/', async () => {
    const tenants = await knex('tenants').select()
    return tenants.map(({ id, anon_key, database_url, jwt_secret, service_key }) => ({
      id,
      anonKey: decrypt(anon_key),
      databaseUrl: decrypt(database_url),
      jwtSecret: decrypt(jwt_secret),
      serviceKey: decrypt(service_key),
    }))
  })

  fastify.get<tenantRequestInterface>('/:tenantId', async (request, reply) => {
    const tenant = await knex('tenants').first().where('id', request.params.tenantId)
    if (!tenant) {
      reply.code(404).send()
    } else {
      const { anon_key, database_url, jwt_secret, service_key } = tenant
      return {
        anonKey: decrypt(anon_key),
        databaseUrl: decrypt(database_url),
        jwtSecret: decrypt(jwt_secret),
        serviceKey: decrypt(service_key),
      }
    }
  })

  fastify.post<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    await knex('tenants').insert({
      id: request.params.tenantId,
      anon_key: encrypt(request.body.anonKey),
      database_url: encrypt(request.body.databaseUrl),
      jwt_secret: encrypt(request.body.jwtSecret),
      service_key: encrypt(request.body.serviceKey),
    })
    reply.code(201).send()
  })

  fastify.patch<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    await knex('tenants')
      .update({
        anon_key: encrypt(request.body.anonKey),
        database_url: encrypt(request.body.databaseUrl),
        jwt_secret: encrypt(request.body.jwtSecret),
        service_key: encrypt(request.body.serviceKey),
      })
      .where('id', request.params.tenantId)
    reply.code(204).send()
  })

  fastify.put<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    await knex('tenants')
      .insert({
        id: request.params.tenantId,
        anon_key: encrypt(request.body.anonKey),
        database_url: encrypt(request.body.databaseUrl),
        jwt_secret: encrypt(request.body.jwtSecret),
        service_key: encrypt(request.body.serviceKey),
      })
      .onConflict('id')
      .merge()
    reply.code(204).send()
  })

  fastify.delete<tenantRequestInterface>('/:tenantId', async (request, reply) => {
    await knex('tenants').del().where('id', request.params.tenantId)
    deleteTenantConfig(request.params.tenantId)
    reply.code(204).send()
  })
}
