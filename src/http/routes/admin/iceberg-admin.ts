import { randomUUID } from 'node:crypto'
import { multitenantPgExecutor } from '@internal/database'
import { SYSTEM_TENANT } from '@internal/queue'
import { DeleteIcebergResources } from '@storage/events/iceberg'
import { ReclaimIcebergShardSlots } from '@storage/events/iceberg/reclaim-shard-slots'
import { UUID_PATTERN } from '@storage/limits'
import { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import { registerApiKeyAuth } from '../../plugins/apikey'

const { isMultitenant, pgQueueEnable } = getConfig()

interface IcebergCatalogRow {
  id: string
  name: string
  tenant_id: string
  deleted_at: string
}

function getOrphanIcebergCatalogs() {
  return multitenantPgExecutor.query<IcebergCatalogRow>(
    `
      SELECT id, name, tenant_id, deleted_at
      FROM iceberg_catalogs
      WHERE deleted_at IS NOT NULL
        AND deleted_at < NOW() - INTERVAL '24 hours'
      ORDER BY deleted_at ASC
    `
  )
}

export default async function routes(fastify: FastifyInstance) {
  registerApiKeyAuth(fastify)

  fastify.post<{
    Body: { dryRun?: boolean; tenantId?: string; shardId?: string; afterReservationId?: string }
  }>(
    '/iceberg/reclaim-shard-slots',
    {
      schema: {
        tags: ['iceberg'],
        body: {
          type: 'object',
          additionalProperties: false,
          properties: {
            dryRun: { type: 'boolean', default: true },
            tenantId: { type: 'string', minLength: 1, maxLength: 255 },
            shardId: { type: 'string', pattern: '^[1-9][0-9]{0,17}$' },
            afterReservationId: {
              type: 'string',
              pattern: UUID_PATTERN,
              description:
                'Scan reservation IDs greater than this UUID. Earlier allocations remain unresolved.',
            },
          },
        },
      },
    },
    async (request, reply) => {
      const { isMultitenant, pgQueueEnable } = getConfig()
      if (!isMultitenant || !pgQueueEnable) {
        return reply.code(400).send({
          error: 'This endpoint only supports multitenant mode with the queue enabled',
        })
      }
      const runId = randomUUID()
      const dryRun = request.body.dryRun !== false
      const jobId = await ReclaimIcebergShardSlots.send({
        runId,
        dryRun,
        tenantId: request.body.tenantId,
        shardId: request.body.shardId,
        afterReservationId: request.body.afterReservationId,
        tenant: SYSTEM_TENANT,
        sbReqId: request.sbReqId,
      })
      if (!jobId) return reply.code(409).send({ error: 'Reclamation job was not queued' })
      return reply.code(202).send({ runId, jobId, dryRun })
    }
  )

  fastify.get(
    '/iceberg/orphan-catalogs',
    { schema: { tags: ['iceberg'] } },
    async (_request, reply) => {
      if (!isMultitenant || !pgQueueEnable) {
        return reply
          .status(400)
          .send({ error: 'This endpoint only supports multitenant mode with the queue enabled' })
      }

      const { rows } = await getOrphanIcebergCatalogs()

      return reply.send({
        count: rows.length,
        items: rows,
      })
    }
  )

  fastify.delete(
    '/iceberg/orphan-catalogs',
    { schema: { tags: ['iceberg'] } },
    async (request, reply) => {
      if (!isMultitenant || !pgQueueEnable) {
        return reply
          .status(400)
          .send({ error: 'This endpoint only supports multitenant mode with the queue enabled' })
      }

      const { rows } = await getOrphanIcebergCatalogs()

      if (rows.length === 0) {
        return reply.status(404).send({
          error: 'No orphan catalogs found to cleanup',
        })
      }

      await DeleteIcebergResources.batchSend(
        rows.map(
          (catalog) =>
            new DeleteIcebergResources({
              catalogId: catalog.id,
              tenant: {
                ref: catalog.tenant_id,
                host: '', // Not needed for cleanup
              },
              sbReqId: request.sbReqId,
            })
        )
      )

      return reply.send({
        message: 'Cleanup jobs scheduled',
        count: rows.length,
        items: rows,
      })
    }
  )
}
