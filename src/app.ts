import fastifySwagger from '@fastify/swagger'
import fastifySwaggerUi from '@fastify/swagger-ui'
import { bucketSchema, objectSchema } from '@storage/schemas'
import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify'
import { getConfig } from './config'
import { plugins, routes, schemas, setErrorHandler } from './http'
import {
  createOpenApiTransform,
  dedupeTrailingSlashPaths,
  nameSchemaByDollarId,
} from './http/routes/openapi-transform'

interface buildOpts extends FastifyServerOptions {
  exposeDocs?: boolean
}

const { version, keepAliveTimeout, headersTimeout, isMultitenant } = getConfig()

const build = (opts: buildOpts = {}): FastifyInstance => {
  const app = fastify(opts)

  app.addContentTypeParser('*', function (request, payload, done) {
    done(null)
  })

  app.server.keepAliveTimeout = keepAliveTimeout * 1000
  app.server.headersTimeout = headersTimeout * 1000

  // kong should take care of cors
  // app.register(fastifyCors)

  if (opts.exposeDocs) {
    app.register(fastifySwagger, {
      exposeHeadRoutes: true,
      transform: createOpenApiTransform(),
      transformObject: dedupeTrailingSlashPaths,
      refResolver: { buildLocalReference: nameSchemaByDollarId },
      openapi: {
        info: {
          title: 'Supabase Storage API',
          description: 'API documentation for Supabase Storage',
          version,
        },
        components: {
          securitySchemes: {
            bearerAuth: {
              type: 'http',
              scheme: 'bearer',
              bearerFormat: 'jwt',
            },
          },
        },
        tags: [
          { name: 'object', description: 'Object end-points' },
          { name: 'bucket', description: 'Bucket end-points' },
          {
            name: 's3',
            description:
              'S3-compatible protocol. Not enumerated here: each operation is dispatched ' +
              'by query string/header on a handful of shared routes, which OpenAPI cannot ' +
              'express as distinct operations. See src/http/routes/s3/commands for the ' +
              'per-command request/response contract, or use any S3 SDK against this endpoint.',
          },
          { name: 'transformation', description: 'Image transformation' },
          { name: 'resumable', description: 'Resumable Upload end-points' },
          { name: 'cdn', description: 'CDN cache management' },
          { name: 'health', description: 'Health check end-points' },
          { name: 'iceberg', description: 'Apache Iceberg REST catalog' },
          { name: 'vector', description: 'Vector storage and search' },
        ],
      },
    })

    app.register(fastifySwaggerUi, {
      routePrefix: '/documentation',
    })
  }

  const excludedRoutesFromMonitoring = [
    '/status',
    '/metrics',
    '/health',
    '/healthcheck',
    '/version',
    '/documentation',
  ]

  // add in common schemas
  app.addSchema(schemas.authSchema)
  app.addSchema(schemas.errorSchema)
  app.addSchema(bucketSchema)
  app.addSchema(objectSchema)

  app.register(plugins.requestContext)
  app.register(plugins.signals)
  app.register(plugins.tenantId)
  app.register(
    plugins.metrics({
      enabledEndpoint: !isMultitenant,
      excludeRoutes: excludedRoutesFromMonitoring,
    })
  )
  app.register(plugins.tracing)
  app.register(plugins.logRequest({ excludeUrls: excludedRoutesFromMonitoring }))
  app.register(plugins.headerValidator({ excludeUrls: excludedRoutesFromMonitoring }))
  app.register(routes.tus, { prefix: 'upload/resumable' })
  app.register(routes.bucket, { prefix: 'bucket' })
  app.register(routes.object, { prefix: 'object' })
  app.register(routes.render, { prefix: 'render/image' })
  app.register(routes.s3, { prefix: 's3' })
  app.register(routes.cdn, { prefix: 'cdn' })
  app.register(routes.healthcheck, { prefix: 'health' })
  app.register(routes.iceberg, { prefix: 'iceberg' })
  app.register(routes.vector, { prefix: 'vector' })

  setErrorHandler(app)

  app.get('/version', (_, reply) => {
    reply.send(version)
  })
  app.get('/status', async (request, response) => response.status(200).send())

  return app
}

export default build
