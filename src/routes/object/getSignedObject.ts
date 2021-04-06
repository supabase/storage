import { FastifyInstance } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { SignedToken } from '../../types/types'
import { verifyJWT } from '../../utils/'
import { getConfig } from '../../utils/config'
import { createResponse } from '../../utils/generic-routes'
import { getObject, initClient } from '../../utils/s3'

const { region, projectRef, globalS3Bucket, globalS3Endpoint } = getConfig()
const client = initClient(region, globalS3Endpoint)

const getSignedObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', example: 'avatars' },
    '*': { type: 'string', example: 'folder/cat.png' },
  },
  required: ['bucketName', '*'],
} as const
const getSignedObjectQSSchema = {
  type: 'object',
  properties: {
    token: {
      type: 'string',
      example:
        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cmwiOiJidWNrZXQyL3B1YmxpYy9zYWRjYXQtdXBsb2FkMjMucG5nIiwiaWF0IjoxNjE3NzI2MjczLCJleHAiOjE2MTc3MjcyNzN9.uBQcXzuvXxfw-9WgzWMBfE_nR3VOgpvfZe032sfLSSk',
    },
  },
  required: ['token'],
} as const

interface GetSignedObjectRequestInterface {
  Params: FromSchema<typeof getSignedObjectParamsSchema>
  Querystring: FromSchema<typeof getSignedObjectQSSchema>
}

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object via a presigned URL'
  fastify.get<GetSignedObjectRequestInterface>(
    '/sign/:bucketName/*',
    {
      // @todo add success response schema here
      schema: {
        params: getSignedObjectParamsSchema,
        querystring: getSignedObjectQSSchema,
        summary,
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      const { token } = request.query
      try {
        const payload = await verifyJWT(token)
        const { url } = payload as SignedToken
        const s3Key = `${projectRef}/${url}`
        request.log.info(s3Key)
        const data = await getObject(client, globalS3Bucket, s3Key)

        return response
          .status(data.$metadata.httpStatusCode ?? 200)
          .header('Content-Type', data.ContentType)
          .header('Cache-Control', data.CacheControl ?? 'no-cache')
          .header('ETag', data.ETag)
          .header('Last-Modified', data.LastModified)
          .send(data.Body)
      } catch (err) {
        request.log.error(err)
        return response.status(400).send(createResponse(err.message, '400', err.name))
      }
    }
  )
}
