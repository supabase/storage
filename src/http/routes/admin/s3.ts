import { FastifyInstance, RequestGenericInterface } from 'fastify'
import apiKey from '../../plugins/apikey'
import {
  createS3Credentials,
  deleteS3Credential,
  listS3Credentials,
} from '../../../internal/database'
import { FromSchema } from 'json-schema-to-ts'

const createCredentialsSchema = {
  description: 'Create S3 Credentials',
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
      description: { type: 'string', minLength: 3, maxLength: 2000 },
      claims: {
        type: 'object',
        properties: {
          role: { type: 'string' },
          sub: { type: 'string' },
        },
        required: ['role'],
        additionalProperties: true,
      },
    },
    required: ['description'],
  },
} as const

const deleteCredentialsSchema = {
  description: 'Delete S3 Credentials',
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
      id: { type: 'string' },
    },
    required: ['id'],
  },
} as const

const listCredentialsSchema = {
  description: 'List S3 Credentials',
  params: {
    type: 'object',
    properties: {
      tenantId: { type: 'string' },
    },
    required: ['tenantId'],
  },
} as const

interface CreateCredentialsRequest extends RequestGenericInterface {
  Body: FromSchema<typeof createCredentialsSchema.body>
  Params: {
    tenantId: string
  }
}

interface DeleteCredentialsRequest extends RequestGenericInterface {
  Body: FromSchema<typeof deleteCredentialsSchema.body>
  Params: {
    tenantId: string
  }
}

interface ListCredentialsRequest extends RequestGenericInterface {
  Params: {
    tenantId: string
  }
}

export default async function routes(fastify: FastifyInstance) {
  fastify.register(apiKey)

  fastify.post<CreateCredentialsRequest>(
    '/:tenantId/credentials',
    {
      schema: createCredentialsSchema,
    },
    async (req, reply) => {
      const credentials = await createS3Credentials(req.params.tenantId, {
        description: req.body.description,
        claims: req.body.claims,
      })

      reply.status(201).send({
        id: credentials.id,
        access_key: credentials.access_key,
        secret_key: credentials.secret_key,
        description: req.body.description,
      })
    }
  )

  fastify.get<ListCredentialsRequest>(
    '/:tenantId/credentials',
    { schema: listCredentialsSchema },
    async (req, reply) => {
      const credentials = await listS3Credentials(req.params.tenantId)

      return reply.send(credentials)
    }
  )

  fastify.delete<DeleteCredentialsRequest>(
    '/:tenantId/credentials',
    { schema: deleteCredentialsSchema },
    async (req, reply) => {
      await deleteS3Credential(req.params.tenantId, req.body.id)

      return reply.code(204).send()
    }
  )
}
