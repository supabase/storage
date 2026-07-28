import { FastifySchema, RouteOptions } from 'fastify'
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

  it.each(multipartOperations)('documents the multipart body for %s', (operation) => {
    const transform = createOpenApiTransform()
    const { schema } = transform({
      schema: {} as FastifySchema,
      url: '/object/:bucketName/*',
      route: routeWithOperation(operation),
    })

    expect(schema.consumes).toEqual(['multipart/form-data'])
    expect(schema.body).toEqual({
      type: 'object',
      properties: {
        cacheControl: { type: 'string', description: "Defaults to 'no-cache' if not set." },
        metadata: { type: 'string' },
        file: { type: 'string', contentEncoding: 'binary' },
      },
      required: ['file'],
    })
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

  it('throws on a genuine operationId collision instead of silently suffixing', () => {
    const transform = createOpenApiTransform()
    transform({
      schema: {},
      url: '/object/:bucketName',
      route: routeWithMethod('GET', ROUTE_OPERATIONS.GET_AUTH_OBJECT),
    })

    expect(() =>
      transform({
        schema: {},
        url: '/object/other',
        route: routeWithMethod('POST', ROUTE_OPERATIONS.GET_AUTH_OBJECT),
      })
    ).toThrow(/Duplicate OpenAPI operationId/)
  })
})
