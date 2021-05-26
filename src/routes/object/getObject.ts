import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { IncomingMessage, Server, ServerResponse } from 'http'
import { AuthenticatedRequest, Obj } from '../../types/types'
import { getPostgrestClient, isValidKey, transformPostgrestError } from '../../utils'
import { getConfig } from '../../utils/config'
import { createResponse } from '../../utils/generic-routes'
import { getObject, initClient } from '../../utils/s3'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const getObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', example: 'avatars' },
    '*': { type: 'string', example: 'folder/cat.png' },
  },
  required: ['bucketName', '*'],
} as const
interface getObjectRequestInterface extends AuthenticatedRequest {
  Params: FromSchema<typeof getObjectParamsSchema>
}

async function requestHandler(request: FastifyRequest<getObjectRequestInterface, Server, IncomingMessage>, response: FastifyReply<Server, IncomingMessage, ServerResponse, getObjectRequestInterface, unknown>) {
  const authHeader = request.headers.authorization
      const jwt = authHeader.substring('Bearer '.length)

      const postgrest = getPostgrestClient(jwt)

      const { bucketName } = request.params
      const objectName = request.params['*']

      if (!isValidKey(objectName) || !isValidKey(bucketName)) {
        return response
          .status(400)
          .send(createResponse('The key contains invalid characters', '400', 'Invalid key'))
      }

      const objectResponse = await postgrest
        .from<Obj>('objects')
        .select('id')
        .match({
          name: objectName,
          bucket_id: bucketName,
        })
        .single()

      if (objectResponse.error) {
        const { status, error } = objectResponse
        request.log.error({ error }, 'error object')
        return response.status(400).send(transformPostgrestError(error, status))
      }

      // send the object from s3
      const s3Key = `${projectRef}/${bucketName}/${objectName}`
      request.log.info(s3Key)
      const data = await getObject(client, globalS3Bucket, s3Key)

      return response
        .status(data.$metadata.httpStatusCode ?? 200)
        .header('Content-Type', data.ContentType)
        .header('Cache-Control', data.CacheControl)
        .header('ETag', data.ETag)
        .header('Last-Modified', data.LastModified)
        .send(data.Body)
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object'
  fastify.get<getObjectRequestInterface>(
    '/authenticated/:bucketName/*',
    {
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
     return requestHandler(request, response) 
    }
  )

  // to be deprecated
  fastify.get<getObjectRequestInterface>(
    '/:bucketName/*',
    {
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary: "Deprecated (use /authenticated/bucketName/object instead): Retrieve an object",
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
     return requestHandler(request, response) 
    }
  )
}
