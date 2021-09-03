import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import apiKey from '../../plugins/apikey'
import { decrypt, encrypt } from '../../utils/crypto'
import { pool } from '../../utils/multitenant-db'
import { cacheTenantConfigAndRunMigrations, deleteTenantConfig } from '../../utils/tenant'

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
    const result = await pool.query(
      `
      SELECT
        id,
        anon_key,
        database_url,
        jwt_secret,
        service_key
      FROM
        tenants
      `
    )
    return result.rows.map(({ id, anon_key, database_url, jwt_secret, service_key }) => ({
      id,
      anonKey: decrypt(anon_key),
      databaseUrl: decrypt(database_url),
      jwtSecret: decrypt(jwt_secret),
      serviceKey: decrypt(service_key),
    }))
  })

  fastify.get<tenantRequestInterface>('/:tenantId', async (request, reply) => {
    const result = await pool.query(
      `
      SELECT
        anon_key,
        database_url,
        jwt_secret,
        service_key
      FROM
        tenants
      WHERE
        id = $1
      `,
      [request.params.tenantId]
    )
    if (result.rows.length === 0) {
      reply.code(404).send()
    } else {
      const { anon_key, database_url, jwt_secret, service_key } = result.rows[0]
      return {
        anonKey: decrypt(anon_key),
        databaseUrl: decrypt(database_url),
        jwtSecret: decrypt(jwt_secret),
        serviceKey: decrypt(service_key),
      }
    }
  })

  fastify.post<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    await pool.query(
      `
      INSERT INTO tenants (id, anon_key, database_url, jwt_secret, service_key)
        VALUES ($1, $2, $3, $4, $5)
      `,
      [
        request.params.tenantId,
        encrypt(request.body.anonKey),
        encrypt(request.body.databaseUrl),
        encrypt(request.body.jwtSecret),
        encrypt(request.body.serviceKey),
      ]
    )
    await cacheTenantConfigAndRunMigrations(request.params.tenantId, request.body)
    reply.code(201).send()
  })

  fastify.patch<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    await pool.query(
      `
      UPDATE
        tenants
      SET
        anon_key = $2,
        database_url = $3,
        jwt_secret = $4,
        service_key = $5
      WHERE
        id = $1
      `,
      [
        request.params.tenantId,
        encrypt(request.body.anonKey),
        encrypt(request.body.databaseUrl),
        encrypt(request.body.jwtSecret),
        encrypt(request.body.serviceKey),
      ]
    )
    await cacheTenantConfigAndRunMigrations(request.params.tenantId, request.body)
    reply.code(204).send()
  })

  fastify.put<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    await pool.query(
      `
      INSERT INTO tenants (id, anon_key, database_url, jwt_secret, service_key)
        VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (id)
        DO UPDATE SET
          anon_key = EXCLUDED.anon_key,
          database_url = EXCLUDED.database_url,
          jwt_secret = EXCLUDED.jwt_secret,
          service_key = EXCLUDED.service_key
      `,
      [
        request.params.tenantId,
        encrypt(request.body.anonKey),
        encrypt(request.body.databaseUrl),
        encrypt(request.body.jwtSecret),
        encrypt(request.body.serviceKey),
      ]
    )
    await cacheTenantConfigAndRunMigrations(request.params.tenantId, request.body)
    reply.code(204).send()
  })

  fastify.delete<tenantRequestInterface>('/:tenantId', async (request, reply) => {
    await pool.query(
      `
      DELETE FROM tenants
      WHERE id = $1
      `,
      [request.params.tenantId]
    )
    deleteTenantConfig(request.params.tenantId)
    reply.code(204).send()
  })
}
