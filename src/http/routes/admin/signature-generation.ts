import { tenantHasMigrations } from '@internal/database/migrations'
import { GenerateObjectSignatures } from '@storage/events'
import { FastifyInstance, RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { registerApiKeyAuth } from '../../plugins/apikey'
import { registerJsonParserAllowingEmptyBody } from '../../plugins/empty-json-body'

const { pgQueueEnable } = getConfig()

const MAX_OBJECT_NAMES = 1000

const generateSignaturesSchema = {
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string' },
    },
    required: ['tenantId'],
  },
  body: {
    type: 'object',
    properties: {
      bucketId: { type: 'string', minLength: 1 },
      objectNames: {
        type: 'array',
        items: { type: 'string', minLength: 1 },
        minItems: 1,
        maxItems: MAX_OBJECT_NAMES,
      },
      force: { type: 'boolean', default: false },
    },
    additionalProperties: false,
  },
} as const

interface GenerateSignaturesRequest extends RequestGenericInterface {
  Params: FromSchema<typeof generateSignaturesSchema.params>
  Body: {
    bucketId?: string
    objectNames?: string[]
    force?: boolean
  }
}

export default async function routes(fastify: FastifyInstance) {
  registerApiKeyAuth(fastify)

  fastify.register(async (f) => {
    registerJsonParserAllowingEmptyBody(f)

    f.post<GenerateSignaturesRequest>(
      '/:tenantId/storage/generate-signatures',
      {
        schema: { ...generateSignaturesSchema, tags: ['object'] },
        preValidation: async (req) => {
          req.body = req.body ?? {}
        },
      },
      async (req, reply) => {
        if (!pgQueueEnable) {
          return reply.status(400).send({ message: 'Queue is not enabled' })
        }

        if (req.body?.objectNames && !req.body.bucketId) {
          return reply.status(400).send({ message: 'bucketId is required when objectNames is set' })
        }

        const hasSignatureMigration = await tenantHasMigrations(
          req.params.tenantId,
          'add-objects-signature'
        )

        if (!hasSignatureMigration) {
          return reply.status(400).send({
            message:
              'Tenant migrations must include add-objects-signature before generating signatures',
          })
        }

        if (!req.body?.objectNames) {
          const hasSignatureIndexMigration = await tenantHasMigrations(
            req.params.tenantId,
            'add-objects-signature-index'
          )

          if (!hasSignatureIndexMigration) {
            return reply.status(400).send({
              message:
                'Tenant migrations must include add-objects-signature-index before broad signature generation',
            })
          }
        }

        const jobId = await GenerateObjectSignatures.send({
          tenant: { ref: req.params.tenantId, host: '' },
          bucketId: req.body?.bucketId,
          objectNames: req.body?.objectNames,
          force: req.body?.force,
          reqId: req.id,
          sbReqId: req.sbReqId,
        })

        return reply.send({
          message: 'Object signature generation scheduled',
          jobId,
        })
      }
    )
  })
}
