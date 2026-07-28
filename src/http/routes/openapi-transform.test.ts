import { RouteOptions } from 'fastify'
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
