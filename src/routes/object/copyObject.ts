import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { getOwner, getPostgrestClient, isValidKey, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createDefaultSchema, createResponse } from '../../utils/generic-routes'
import { copyObject, initClient } from '../../utils/s3'

const { region, projectRef, globalS3Bucket, globalS3Endpoint, serviceKey } = getConfig()
const client = initClient(region, globalS3Endpoint)

const copyRequestBodySchema = {
  type: 'object',
  properties: {
    sourceKey: { type: 'string', example: 'folder/source.png' },
    bucketName: { type: 'string', example: 'avatars' },
    destinationKey: { type: 'string', example: 'folder/destination.png' },
  },
  required: ['sourceKey', 'bucketName', 'destinationKey'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    Key: { type: 'string', example: 'folder/destination.png' },
  },
  required: ['Key'],
}
interface copyRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof copyRequestBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Copies an object'

  const schema = createDefaultSchema(successResponseSchema, {
    body: copyRequestBodySchema,
    summary,
    tags: ['object'],
  })

  fastify.post<copyRequestInterface>(
    '/copy',
    {
      schema,
    },
    async (request, response) => {
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const { sourceKey, destinationKey, bucketName } = request.body
      request.log.info(
        'sourceKey is %s and bucketName is %s and destinationKey is %s',
        sourceKey,
        bucketName,
        destinationKey
      )

      if (!isValidKey(destinationKey)) {
        const responseValue = createResponse(
          'The destination key contains invalid characters',
          '400',
          'Invalid key'
        )

        request.log.error(responseValue)
        return response.status(400).send(responseValue)
      }

      const postgrest = getPostgrestClient(jwt)
      const superUserPostgrest = getPostgrestClient(serviceKey)

      let owner
      try {
        owner = await getOwner(jwt)
      } catch (err) {
        request.log.error(err)
        return response.status(400).send(createResponse(err.message, '400', err.message))
      }
      const objectResponse = await superUserPostgrest
        .from<Obj>('objects')
        .select('bucket_id, metadata')
        .match({
          name: sourceKey,
          bucket_id: bucketName,
        })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      const { data: origObject } = objectResponse
      request.log.info({ origObject }, 'origObject')

      const newObject = Object.assign({}, origObject, {
        name: destinationKey,
        owner,
      })
      request.log.info({ origObject }, 'newObject')
      const { data: results, error, status } = await postgrest
        .from<Obj>('objects')
        .insert([newObject], {
          returning: 'minimal',
        })
        .single()

      if (error) {
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }
      request.log.info({ results }, 'results')

      const s3SourceKey = `${projectRef}/${bucketName}/${sourceKey}`
      const s3DestinationKey = `${projectRef}/${bucketName}/${destinationKey}`
      const copyResult = await copyObject(client, globalS3Bucket, s3SourceKey, s3DestinationKey)
      return response.status(copyResult.$metadata.httpStatusCode ?? 200).send({
        Key: `${bucketName}/${destinationKey}`,
      })
    }
  )
}
