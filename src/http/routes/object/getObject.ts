import { ERRORS } from '@internal/errors'
import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import { authHeadersSchema } from '../../schemas/auth'
import { sharedErrorResponseSchemas } from '../../schemas/error'
import { AuthenticatedRangeRequest } from '../../types'
import { ROUTE_OPERATIONS } from '../operations'

const { storageS3Bucket } = getConfig()

const getObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const

const getObjectQuerySchema = {
  type: 'object',
  properties: {
    download: { type: 'string', examples: ['filename.jpg', null] },
    versionId: { type: 'string', examples: ['eaa8bdb5-2e00-4767-b5a9-d2502efe2196'] },
  },
} as const

interface getObjectRequestInterface extends AuthenticatedRangeRequest {
  Params: FromSchema<typeof getObjectParamsSchema>
  Querystring: FromSchema<typeof getObjectQuerySchema>
}

type GetObjectRequest = FastifyRequest<getObjectRequestInterface>

async function requestHandler(request: GetObjectRequest, response: FastifyReply) {
  const { bucketName } = request.params
  const { download, versionId } = request.query
  const objectName = request.params['*']

  // send the object from s3
  const s3Key = request.storage.location.getKeyLocation({
    tenantId: request.tenantId,
    bucketId: bucketName,
    objectName,
  })
  // The bucket is read as the super user; the object as the super user for a public
  // bucket, with RLS otherwise. Both reads usually cost one round trip.
  const lookup = await request.storage.findBucketAndObject(
    bucketName,
    objectName,
    'id, version, metadata',
    { version: versionId, publicBucketOnly: !request.isAuthenticated }
  )
  const bucket = lookup.bucket

  // The request is not authenticated
  if (!request.isAuthenticated) {
    // The bucket must be public to access its content
    if (!bucket?.public) {
      throw ERRORS.NoSuchBucket(bucketName)
    }
  }

  // The request is authenticated
  if (!bucket) {
    throw ERRORS.NoSuchBucket(bucketName)
  }

  const obj = await lookup.findObject()

  return request.storage.renderer('asset').render(request, response, {
    bucket: storageS3Bucket,
    key: s3Key,
    version: obj.version,
    download,
    xRobotsTag: obj.metadata?.['xRobotsTag'] as string | undefined,
    signal: request.signals.disconnect.signal,
  })
}

export default async function routes(fastify: FastifyInstance) {
  const summary = 'Retrieve an object'
  fastify.get<getObjectRequestInterface>(
    '/authenticated/:bucketName/*',
    {
      exposeHeadRoute: false,
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        querystring: getObjectQuerySchema,
        headers: authHeadersSchema,
        summary,
        response: sharedErrorResponseSchemas,
        tags: ['object'],
      },
      config: {
        operation: ROUTE_OPERATIONS.GET_AUTH_OBJECT,
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )

  fastify.get<getObjectRequestInterface>(
    '/:bucketName/*',
    {
      exposeHeadRoute: false,
      // @todo add success response schema here
      schema: {
        params: getObjectParamsSchema,
        querystring: getObjectQuerySchema,
        summary,
        description: 'Serve objects',
        tags: ['object'],
        response: sharedErrorResponseSchemas,
      },
      config: {
        operation: ROUTE_OPERATIONS.GET_AUTH_OBJECT,
        allowInvalidJwt: true,
      },
    },
    async (request, response) => {
      return requestHandler(request, response)
    }
  )
}
