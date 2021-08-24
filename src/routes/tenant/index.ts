import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
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
  fastify.get('/', async () => {
    const result = await pool.query(
      `
      SELECT
        id,
        config
      FROM
        tenants
      `
    )
    return result.rows
  })

  fastify.get<tenantRequestInterface>('/:tenantId', async (request, reply) => {
    const result = await pool.query(
      `
      SELECT
        config
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
      return result.rows[0].config
    }
  })

  fastify.post<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    await pool.query(
      `
      INSERT INTO tenants (id, config)
        VALUES ($1, $2)
      `,
      [request.params.tenantId, request.body]
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
        config = $2
      WHERE
        id = $1
      `,
      [request.params.tenantId, request.body]
    )
    await cacheTenantConfigAndRunMigrations(request.params.tenantId, request.body)
    reply.code(204).send()
  })

  fastify.put<tenantRequestInterface>('/:tenantId', { schema }, async (request, reply) => {
    await pool.query(
      `
      INSERT INTO tenants (id, config)
        VALUES ($1, $2)
      ON CONFLICT (id)
        DO UPDATE SET
          config = EXCLUDED.config
      `,
      [request.params.tenantId, request.body]
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
