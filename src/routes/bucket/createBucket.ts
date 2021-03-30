import { FastifyInstance } from 'fastify'
import { getPostgrestClient, getOwner, transformPostgrestError, isValidKey } from '../../utils'
import { AuthenticatedRequest, Bucket } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'

const createBucketBodySchema = {
  type: 'object',
  properties: {
    name: { type: 'string' },
    id: { type: 'string' },
  },
  required: ['name'],
} as const

const successResponseSchema = {
  type: 'object',
  properties: {
    name: { type: 'string' },
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
  })
  fastify.post<createBucketRequestInterface>(
    '/',
    {
      schema,
    },
    async (request, response) => {
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)
      const postgrest = getPostgrestClient(jwt)
      let owner
      try {
        owner = await getOwner(jwt)
      } catch (err) {
        console.log(err)
        return response.status(400).send()
      }

      const { name: bucketName } = request.body

      // IMPORTANT: by default set the id as the name of the bucket
      const id = request.body.id || bucketName

      if (!isValidKey(id) || !isValidKey(bucketName)) {
        return response
          .status(400)
          .send(createResponse('The key contains invalid characters', '400', 'Invalid key'))
      }

      const { data: results, error, status } = await postgrest
        .from<Bucket>('buckets')
        .insert(
          [
            {
              id,
              name: bucketName,
              owner,
            },
          ],
          {
            returning: 'minimal',
          }
        )
        .single()

      if (error) {
        request.log.error({ error }, 'error bucket')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')
      return response.status(200).send({
        name: bucketName,
      })
    }
  )
}
