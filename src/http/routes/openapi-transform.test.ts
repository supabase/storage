import { FastifySchema, RouteOptions } from 'fastify'
import { sharedErrorResponseSchemas } from '../schemas/error'
import { createOpenApiTransform } from './openapi-transform'
import { ROUTE_OPERATIONS } from './operations'

function routeWithOperation(operation?: string): RouteOptions {
  return {
    method: 'POST',
    url: '/object/:bucketName/*',
    config: operation ? { operation } : undefined,
  } as RouteOptions
}

describe('documentMultipartUploadBody (via transformOpenApiSchema)', () => {
  const multipartOperations = [
    ROUTE_OPERATIONS.CREATE_OBJECT,
    ROUTE_OPERATIONS.UPDATE_OBJECT,
    ROUTE_OPERATIONS.UPLOAD_SIGN_OBJECT,
  ]

  it.each(multipartOperations)('documents the multipart form body for %s', (operation) => {
    const transform = createOpenApiTransform()
    const { schema } = transform({
      schema: {} as FastifySchema,
      url: '/object/:bucketName/*',
      route: routeWithOperation(operation),
    })

    const body = schema.body as { content: Record<string, { schema: unknown }> }
    expect(body.content['multipart/form-data'].schema).toEqual({
      type: 'object',
      properties: {
        cacheControl: { type: 'string', description: "Defaults to 'no-cache' if not set." },
        metadata: {
          type: 'string',
          description: 'JSON-encoded custom metadata. Alias: userMetadata.',
        },
        userMetadata: { type: 'string', description: 'Alias for metadata.' },
        contentType: { type: 'string', description: 'Overrides the auto-detected mime type.' },
        file: { type: 'string', format: 'binary' },
      },
      required: ['file'],
    })
  })

  it.each(multipartOperations)('documents the raw (non-multipart) body for %s', (operation) => {
    const transform = createOpenApiTransform()
    const { schema } = transform({
      schema: {} as FastifySchema,
      url: '/object/:bucketName/*',
      route: routeWithOperation(operation),
    })

    const body = schema.body as { content: Record<string, { schema: Record<string, unknown> }> }
    expect(body.content['*/*'].schema.type).toBe('string')
    expect(body.content['*/*'].schema.format).toBe('binary')
    expect(body.content['*/*'].schema.description).toContain('x-metadata')
  })

  it('leaves an unrelated operation unchanged', () => {
    const transform = createOpenApiTransform()
    const { schema } = transform({
      schema: {},
      url: '/object/:bucketName',
      route: routeWithOperation(ROUTE_OPERATIONS.GET_AUTH_OBJECT),
    })

    expect(schema.body).toBeUndefined()
    expect(schema.consumes).toBeUndefined()
  })

  it('leaves a route with no operation unchanged', () => {
    const transform = createOpenApiTransform()
    const { schema } = transform({
      schema: {},
      url: '/object/:bucketName',
      route: routeWithOperation(undefined),
    })

    expect(schema.body).toBeUndefined()
    expect(schema.consumes).toBeUndefined()
  })
})

describe('defaultErrorResponse (via transformOpenApiSchema)', () => {
  function routeAt(url: string): RouteOptions {
    return { method: 'GET', url } as RouteOptions
  }

  it('defaults an undocumented route to the standard error shape', () => {
    const transform = createOpenApiTransform()
    const { schema } = transform({
      schema: {},
      url: '/object/:bucketName',
      route: routeAt('/object/:bucketName'),
    })

    expect(schema.response).toEqual({
      200: { description: 'Default Response' },
      '4xx': { description: 'Error response', $ref: 'errorSchema#' },
    })
  })

  it('leaves iceberg routes undocumented instead of claiming the standard error shape', () => {
    const transform = createOpenApiTransform()
    const { schema } = transform({
      schema: {},
      url: '/iceberg/bucket',
      route: routeAt('/iceberg/bucket'),
    })

    expect(schema.response).toBeUndefined()
  })
})

describe('operationId (via transformOpenApiSchema)', () => {
  function routeWithMethod(method: string, operation: string): RouteOptions {
    return { method, url: '/object/:bucketName', config: { operation } } as RouteOptions
  }

  it('derives an operationId from config.operation', () => {
    const transform = createOpenApiTransform()
    const { schema } = transform({
      schema: {},
      url: '/object/:bucketName',
      route: routeWithMethod('GET', ROUTE_OPERATIONS.GET_AUTH_OBJECT),
    })

    expect(schema.operationId).toBe('objectGetAuthenticated')
  })

  it('prefers an explicit config.operationId over the derived one', () => {
    const transform = createOpenApiTransform()
    const route = routeWithMethod('GET', ROUTE_OPERATIONS.GET_AUTH_OBJECT)
    ;(route.config as { operationId?: string }).operationId = 'customId'
    const { schema } = transform({
      schema: {},
      url: '/object/:bucketName',
      route,
    })

    expect(schema.operationId).toBe('customId')
  })

  it('suffixes an auto-derived HEAD route deterministically instead of by registration order', () => {
    const transform = createOpenApiTransform()
    transform({
      schema: {},
      url: '/object/:bucketName',
      route: routeWithMethod('GET', ROUTE_OPERATIONS.GET_AUTH_OBJECT),
    })
    const { schema } = transform({
      schema: {},
      url: '/object/:bucketName',
      route: routeWithMethod('HEAD', ROUTE_OPERATIONS.GET_AUTH_OBJECT),
    })

    expect(schema.operationId).toBe('objectGetAuthenticatedHead')
  })

  it('warns and leaves a genuine operationId collision undocumented instead of silently suffixing', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const transform = createOpenApiTransform()
    transform({
      schema: {},
      url: '/object/:bucketName',
      route: routeWithMethod('GET', ROUTE_OPERATIONS.GET_AUTH_OBJECT),
    })

    const { schema } = transform({
      schema: {},
      url: '/object/other',
      route: routeWithMethod('POST', ROUTE_OPERATIONS.GET_AUTH_OBJECT),
    })

    expect(schema.operationId).toBeUndefined()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('Duplicate operationId'))
    warn.mockRestore()
  })
})

describe('vector bucket routes (via transformOpenApiSchema)', () => {
  function vectorRoute(method: string, url: string, operation: string): RouteOptions {
    return { method, url, config: { operation } } as RouteOptions
  }

  it('preserves a $ref success response and the shared error envelope for GetVectorBucket', () => {
    const transform = createOpenApiTransform()
    const response = {
      200: { description: 'Successful response', $ref: 'getVectorBucketResponse#' },
      ...sharedErrorResponseSchemas,
    }

    const { schema } = transform({
      schema: { tags: ['vector'], response },
      url: '/vector/GetVectorBucket',
      route: vectorRoute('POST', '/vector/GetVectorBucket', ROUTE_OPERATIONS.GET_VECTOR_BUCKET),
    })

    expect(schema.response).toEqual(response)
    expect(schema.operationId).toBe('vectorBucketGet')
  })

  it('preserves an array $ref success response and the shared error envelope for ListVectorBuckets', () => {
    const transform = createOpenApiTransform()
    const response = {
      200: { description: 'Successful response', $ref: 'listVectorBucketsResponse#' },
      ...sharedErrorResponseSchemas,
    }

    const { schema } = transform({
      schema: { tags: ['vector'], response },
      url: '/vector/ListVectorBuckets',
      route: vectorRoute('POST', '/vector/ListVectorBuckets', ROUTE_OPERATIONS.LIST_VECTOR_BUCKETS),
    })

    expect(schema.response).toEqual(response)
    expect(schema.operationId).toBe('vectorBucketList')
  })

  it('preserves a null-body success response and the shared error envelope for CreateVectorBucket', () => {
    const transform = createOpenApiTransform()
    const response = {
      200: { type: 'null', description: 'Successful response' },
      ...sharedErrorResponseSchemas,
    }

    const { schema } = transform({
      schema: { tags: ['vector'], response },
      url: '/vector/CreateVectorBucket',
      route: vectorRoute(
        'POST',
        '/vector/CreateVectorBucket',
        ROUTE_OPERATIONS.CREATE_VECTOR_BUCKET
      ),
    })

    expect(schema.response).toEqual(response)
    expect(schema.operationId).toBe('vectorBucketCreate')
  })

  it('preserves a null-body success response and the shared error envelope for DeleteVectorBucket', () => {
    const transform = createOpenApiTransform()
    const response = {
      200: { type: 'null', description: 'Successful response' },
      ...sharedErrorResponseSchemas,
    }

    const { schema } = transform({
      schema: { tags: ['vector'], response },
      url: '/vector/DeleteVectorBucket',
      route: vectorRoute(
        'POST',
        '/vector/DeleteVectorBucket',
        ROUTE_OPERATIONS.DELETE_VECTOR_BUCKET
      ),
    })

    expect(schema.response).toEqual(response)
    expect(schema.operationId).toBe('vectorBucketDelete')
  })
})
