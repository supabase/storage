import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { IncomingMessage, Server, ServerResponse } from 'http'
import { getConfig } from '../../../config'
import { AuthenticatedRangeRequest } from '../../request'
import { Obj } from '../../../storage/schemas'
import { getJwtSecret, SignedToken, verifyJWT } from '../../../auth'
import { StorageBackendError } from '../../../storage'

const { storageS3Bucket } = getConfig()

const getObjectParamsSchema = {
  type: 'object',
  properties: {
    bucketName: { type: 'string', examples: ['avatars'] },
    '*': { type: 'string', examples: ['folder/cat.png'] },
  },
  required: ['bucketName', '*'],
} as const

const getSignedObjectQSSchema = {
  type: 'object',
  properties: {
    token: {
      type: 'string',
      examples: [
        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cmwiOiJidWNrZXQyL3B1YmxpYy9zYWRjYXQtdXBsb2FkMjMucG5nIiwiaWF0IjoxNjE3NzI2MjczLCJleHAiOjE2MTc3MjcyNzN9.uBQcXzuvXxfw-9WgzWMBfE_nR3VOgpvfZe032sfLSSk',
      ],
    },
  },
} as const

interface getObjectRequestInterface extends AuthenticatedRangeRequest {
  Params: FromSchema<typeof getObjectParamsSchema>
}

interface getSignObjectRequestInterface extends AuthenticatedRangeRequest {
  Params: FromSchema<typeof getObjectParamsSchema>
  Querystring: FromSchema<typeof getSignedObjectQSSchema>
}

async function handleSignedToken(request: FastifyRequest<getSignObjectRequestInterface>) {
  let payload: SignedToken
  const jwtSecret = await getJwtSecret(request.tenantId)
  const token = request.query.token

  if (!token) {
    throw new StorageBackendError('missing_token', 400, 'Missing token')
  }

  try {
    payload = (await verifyJWT(token, jwtSecret)) as SignedToken

    const { url, exp } = payload
    const bucketName = request.params.bucketName
    const objectName = request.params['*']

    const path = `${bucketName}/${objectName}`

    if (url !== path) {
      throw new StorageBackendError('InvalidSignature', 400, 'The url do not match the signature')
    }

    const obj = await request.storage
      .asSuperUser()
      .from(request.params.bucketName)
      .findObject(objectName, 'id,version')

    return { obj, exp: new Date(exp * 1000).toUTCString() }
  } catch (e) {
    if (e instanceof StorageBackendError) {
      throw e
    }
    const err = e as Error
    throw new StorageBackendError('Invalid JWT', 400, err.message, err)
  }
}

async function requestHandler(
  request: FastifyRequest<getObjectRequestInterface, Server, IncomingMessage>,
  response: FastifyReply<
    Server,
    IncomingMessage,
    ServerResponse,
    getObjectRequestInterface,
    unknown
  >,
  routeVisibility: 'public' | 'private' | 'signed'
) {
  const { bucketName } = request.params
  const objectName = request.params['*']

  const s3Key = `${request.tenantId}/${bucketName}/${objectName}`

  let obj: Obj
  let expires: string | undefined
  switch (routeVisibility) {
    case 'public':
      obj = await request.storage
        .asSuperUser()
        .from(bucketName)
        .findObject(objectName, 'id,version')
      break
    case 'private':
      obj = await request.storage.from(bucketName).findObject(objectName, 'id,version')
      break
    case 'signed':
      const { obj: signedObj, exp } = await handleSignedToken(
        request as FastifyRequest<getSignObjectRequestInterface>
      )
      obj = signedObj
      expires = exp
      break
    default:
      throw new Error(`Invalid route visibility: ${routeVisibility}`)
  }

  return request.storage.renderer('head').render(request, response, {
    bucket: storageS3Bucket,
    key: s3Key,
    version: obj.version,
    expires,
  })
}

export async function publicRoutes(fastify: FastifyInstance) {
  fastify.head<getSignObjectRequestInterface>(
    '/info/sign/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        description: 'returns object info',
        tags: ['object'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response, 'signed')
    }
  )

  fastify.get<getSignObjectRequestInterface>(
    '/info/sign/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        description: 'returns object info',
        tags: ['object'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response, 'signed')
    }
  )

  fastify.head<getObjectRequestInterface>(
    '/public/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        summary: 'Get object info',
        description: 'returns object info',
        tags: ['object'],
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      return requestHandler(request, response, 'public')
    }
  )

  fastify.get<getObjectRequestInterface>(
    '/info/public/:bucketName/*',
    {
      exposeHeadRoute: false,
      schema: {
        params: getObjectParamsSchema,
        summary: 'Get object info',
        description: 'returns object info',
        tags: ['object'],
        response: { '4xx': { $ref: 'errorSchema#' } },
      },
    },
    async (request, response) => {
      return requestHandler(request, response, 'public')
    }
  )
}

export async function authenticatedRoutes(fastify: FastifyInstance) {
  const summary = 'Retrieve object info'

  fastify.head<getObjectRequestInterface>(
    '/authenticated/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response, 'private')
    }
  )

  fastify.get<getObjectRequestInterface>(
    '/info/authenticated/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        response: { '4xx': { $ref: 'errorSchema#', description: 'Error response' } },
        tags: ['object'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response, 'private')
    }
  )

  fastify.get<getObjectRequestInterface>(
    '/info/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        description: 'use HEAD /object/authenticated/{bucketName} instead',
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['deprecated'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response, 'private')
    }
  )

  fastify.head<getObjectRequestInterface>(
    '/:bucketName/*',
    {
      schema: {
        params: getObjectParamsSchema,
        headers: { $ref: 'authSchema#' },
        summary,
        description: 'use HEAD /object/authenticated/{bucketName} instead',
        response: { '4xx': { $ref: 'errorSchema#' } },
        tags: ['deprecated'],
      },
    },
    async (request, response) => {
      return requestHandler(request, response, 'private')
    }
  )
}
