import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Bucket } from '../../types/types'
import { getJwtSecret, getOwner, isValidKey, transformPostgrestError } from '../../utils'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'

const createBucketBodySchema = {
  type: 'object',
  properties: {
    name: { type: 'string', examples: ['avatars'] },
    id: { type: 'string', examples: ['avatars'] },
    public: { type: 'boolean', examples: [false] },
  },
  required: ['name'],
} as const

const successResponseSchema = {
  type: 'object',
  properties: {
    name: { type: 'string', examples: ['avatars'] },
  },
  required: ['name'],
}
interface createBucketRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof createBucketBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Create a bucket'
  const schema = createDefaultSchema(successResponseSchema, {
    body: createBucketBodySchema,
    summary,
    tags: ['bucket'],
  })
  fastify.post<createBucketRequestInterface>(
    '/',
    {
      schema,
    },
    async (request, response) => {
      const jwtSecret = await getJwtSecret(request.tenantId)
      let owner
      try {
        owner = await getOwner(request.jwt, jwtSecret)
      } catch (err: any) {
        request.log.error({ error: err }, 'unable to get owner')
        return response.status(400).send(createResponse(err.message, '400', err.message))
      }

      const { name: bucketName } = request.body

      // by default set the id as the name of the bucket
      const id = request.body.id ?? bucketName

      // by default buckets are not public
      const isPublic = request.body.public ?? false

      if (!isValidKey(id) || !isValidKey(bucketName)) {
        return response
          .status(400)
          .send(createResponse('The key contains invalid characters', '400', 'Invalid key'))
      }

      const {
        data: results,
        error,
        status,
      } = await request.postgrest
        .from<Bucket>('buckets')
        .insert(
          [
            {
              id,
              name: bucketName,
              owner,
              public: isPublic,
            },
          ],
          {
            returning: 'minimal',
          }
        )
        .single()

      if (error) {
        request.log.error({ error }, 'error creating bucket')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')
      return response.status(200).send({
        name: bucketName,
      })
    }
  )
}
