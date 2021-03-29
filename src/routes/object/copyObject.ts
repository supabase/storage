import { FastifyInstance } from 'fastify'
import { getOwner, getPostgrestClient, isValidKey, transformPostgrestError } from '../../utils'
import { copyObject, initClient } from '../../utils/s3'
import { getConfig } from '../../utils/config'
import { Obj, AuthenticatedRequest } from '../../types/types'
import { FromSchema } from 'json-schema-to-ts'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const copyRequestBodySchema = {
  type: 'object',
  properties: {
    sourceKey: { type: 'string' },
    bucketName: { type: 'string' },
    destinationKey: { type: 'string' },
  },
  required: ['sourceKey', 'bucketName', 'destinationKey'],
} as const
const successResponseSchema = {
  type: 'object',
  properties: {
    Key: { type: 'string' },
  },
  required: ['Key'],
}
interface copyRequestInterface extends AuthenticatedRequest {
  Body: FromSchema<typeof copyRequestBodySchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Copies an object'
  fastify.post<copyRequestInterface>(
    '/copy',
    {
      schema: {
        body: copyRequestBodySchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { 200: successResponseSchema, '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const { sourceKey, destinationKey, bucketName } = request.body
      console.log(sourceKey, bucketName)

      if (!isValidKey(destinationKey)) {
        return response.status(400).send({
          statusCode: '400',
          error: 'Invalid key',
          message: 'The destination key contains invalid characters',
        })
      }

      const postgrest = getPostgrestClient(jwt)
      let owner
      try {
        owner = await getOwner(jwt)
      } catch (err) {
        console.log(err)
        return response.status(400).send({
          statusCode: '400',
          error: err.message,
          message: err.message,
        })
      }
      const objectResponse = await postgrest
        .from<Obj>('objects')
        .select('bucket_id, metadata')
        .match({
          name: sourceKey,
          bucket_id: bucketName,
        })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        console.log(error)
        return response.status(400).send(transformPostgrestError(error, status))
      }
      const { data: origObject } = objectResponse
      console.log('origObject', origObject)

      const newObject = Object.assign({}, origObject, {
        name: destinationKey,
        owner,
      })
      console.log(newObject)
      const { data: results, error, status } = await postgrest
        .from<Obj>('objects')
        .insert([newObject], {
          returning: 'minimal',
        })
        .single()

      console.log(results, error)
      if (error) {
        return response.status(400).send(transformPostgrestError(error, status))
      }

      const s3SourceKey = `${projectRef}/${bucketName}/${sourceKey}`
      const s3DestinationKey = `${projectRef}/${bucketName}/${destinationKey}`
      const copyResult = await copyObject(client, globalS3Bucket, s3SourceKey, s3DestinationKey)
      return response.status(copyResult.$metadata.httpStatusCode ?? 200).send({
        Key: destinationKey,
      })
    }
  )
}
