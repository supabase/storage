import { RouteOptions } from 'fastify'
import { sharedErrorResponseSchemas } from '../schemas/error'
import { createOpenApiTransform } from './openapi-transform'
import { ROUTE_OPERATIONS } from './operations'

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

  it('prefers an explicit schema.operationId over the derived one', () => {
    const transform = createOpenApiTransform()
    const { schema } = transform({
      schema: { operationId: 'customId' },
      url: '/object/:bucketName',
      route: routeWithMethod('GET', ROUTE_OPERATIONS.GET_AUTH_OBJECT),
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
