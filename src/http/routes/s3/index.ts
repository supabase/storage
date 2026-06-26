import fastifyMultipart from '@fastify/multipart'
import { ERRORS } from '@internal/errors'
import { FastifyInstance, RouteHandlerMethod } from 'fastify'
import { JSONSchema } from 'json-schema-to-ts'
import { getConfig } from '../../../config'
import {
  db,
  detectS3IcebergBucket,
  icebergRestCatalog,
  requireTenantFeature,
  signatureV4,
  storage,
  xmlParser,
} from '../../plugins'
import { s3ErrorHandler } from './error-handler'
import { findArrayPathsInSchemas, getRouter, RequestInput, RouteQuery } from './router'

const { s3ProtocolEnabled } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  if (!s3ProtocolEnabled) {
    return
  }

  fastify.register(async (fastify) => {
    const s3Router = getRouter()
    const s3Routes = s3Router.routes()

    Array.from(s3Routes.keys()).forEach((routePath) => {
      const routes = s3Routes.get(routePath)
      if (!routes || routes?.length === 0) {
        return
      }

      const methods = new Set(routes.map((e) => e.method))

      methods.forEach((method) => {
        const routesByMethod = routes.filter((e) => e.method === method)
        const icebergRoutes = routesByMethod.filter((e) => e.type === 'iceberg')
        const standardRoutes = routesByMethod.filter((e) => e.type === undefined)

        const routeHandler: RouteHandlerMethod = async (req, reply) => {
          const matchType = req.isIcebergBucket ? 'iceberg' : undefined
          const matchQuery = (req.query as RouteQuery) || {}
          const matchHeaders = (req.headers as Record<string, string>) || {}
          const candidates = matchType === 'iceberg' ? icebergRoutes : standardRoutes

          for (const route of candidates) {
            if (route.matches(matchType, matchQuery, matchHeaders)) {
              if (!route.handler) {
                throw new Error('no handler found')
              }

              if (!route.acceptMultiformData && req.isMultipart()) {
                return reply.status(400).send({
                  message: 'Multipart form data not supported',
                })
              }

              try {
                req.operation = route.operationConfig

                if (req.operation.type && typeof req.opentelemetry === 'function') {
                  req.opentelemetry()?.span?.setAttribute('http.operation', req.operation.type)
                }

                const data = {
                  Params: req.params,
                  Body: req.body,
                  Headers: req.headers,
                  Querystring: req.query,
                } as RequestInput<typeof route.schema>
                const isValid = route.validate(data)

                if (!isValid) {
                  const validationError = ERRORS.InvalidRequest('Invalid request') as Error & {
                    validation?: unknown
                  }
                  // validation property is required to send correct reply in error-handler.ts
                  validationError.validation = route.validate.errors
                  throw validationError
                }

                const output = await route.handler(data, {
                  req,
                  storage: req.storage,
                  tenantId: req.tenantId,
                  owner: req.owner,
                  signals: {
                    get body() {
                      return req.signals.body.signal
                    },
                    get response() {
                      return req.signals.response.signal
                    },
                  },
                })

                const headers = output.headers

                if (headers) {
                  for (const header in headers) {
                    if (!Object.prototype.hasOwnProperty.call(headers, header)) {
                      continue
                    }

                    const value = headers[header]
                    if (value || (value === '' && header.startsWith('x-amz-meta-'))) {
                      reply.header(header, value)
                    }
                  }
                }
                return reply.status(output.statusCode || 200).send(output.responseBody)
              } catch (e) {
                if (route.disableContentTypeParser) {
                  reply.header('connection', 'close')
                  reply.raw.on('finish', () => {
                    // wait sometime so that the client can receive the response
                    setTimeout(() => {
                      if (!req.raw.destroyed) {
                        req.raw.destroy()
                      }
                    }, 3000)
                  })
                }
                throw e
              }
            }
          }

          return reply.status(404).send()
        }

        fastify.register(async (localFastify) => {
          localFastify.register(requireTenantFeature('s3Protocol'))

          const disableContentParser = routesByMethod?.some(
            (route) => route.disableContentTypeParser
          )
          const allowEmptyJsonBody = routesByMethod?.some((route) => route.allowEmptyJsonBody)

          if (disableContentParser) {
            localFastify.addContentTypeParser(
              ['application/json', 'text/plain', 'application/xml'],
              function (request, payload, done) {
                done(null)
              }
            )
          } else if (allowEmptyJsonBody) {
            const defaultJsonParser = localFastify.getDefaultJsonParser(
              localFastify.initialConfig.onProtoPoisoning ?? 'error',
              localFastify.initialConfig.onConstructorPoisoning ?? 'error'
            )

            localFastify.addContentTypeParser(
              'application/json',
              { parseAs: 'string' },
              (request, body, done) => {
                const allowsEmptyBody =
                  (request.query as { uploads?: unknown }).uploads !== undefined

                if (!body && allowsEmptyBody) {
                  done(null, null)
                  return
                }

                const jsonBody = typeof body === 'string' ? body : body.toString('utf8')

                defaultJsonParser(request, jsonBody, done)
              }
            )
          }

          localFastify.register(fastifyMultipart, {
            limits: {
              fields: 20,
              files: 1,
            },
            throwFileSizeLimit: false,
          })

          localFastify.register(signatureV4)
          localFastify.register(xmlParser, {
            disableContentParser,
            parseAsArray: findArrayPathsInSchemas(
              routesByMethod.filter((r) => r.schema.Body).map((r) => r.schema.Body as JSONSchema)
            ),
          })

          localFastify.register(db)
          localFastify.register(icebergRestCatalog)
          localFastify.register(detectS3IcebergBucket)
          localFastify.register(storage)

          localFastify[method](
            routePath,
            {
              validatorCompiler: () => () => true,
              exposeHeadRoute: false,
              schema: {
                tags: ['s3'],
              },
              errorHandler: s3ErrorHandler,
            },
            routeHandler
          )

          // handle optional trailing slash
          if (!routePath.endsWith('*') && !routePath.endsWith('/')) {
            localFastify[method](
              routePath + '/',
              {
                validatorCompiler: () => () => true,
                exposeHeadRoute: false,
                schema: {
                  tags: ['s3'],
                },
                errorHandler: s3ErrorHandler,
              },
              routeHandler
            )
          }
        })
      })
    })
  })
}
