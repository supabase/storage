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
        cacheControl: { type: 'string' },
        metadata: { type: 'string' },
        file: { type: 'string', contentEncoding: 'binary' },
      },
      required: ['cacheControl', 'file'],
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
