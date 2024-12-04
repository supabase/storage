import { FastifyInstance, RouteHandlerMethod } from 'fastify'
import { JSONSchema } from 'json-schema-to-ts'
import { trace } from '@opentelemetry/api'
import { db, jsonToXml, requireTenantFeature, signatureV4, storage } from '../../plugins'
import { findArrayPathsInSchemas, getRouter, RequestInput } from './router'
import { s3ErrorHandler } from './error-handler'
import { getConfig } from '../../../config'

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

      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      const methods = new Set(routes.map((e) => e.method))

      methods.forEach((method) => {
        const routesByMethod = routes.filter((e) => e.method === method)

        const routeHandler: RouteHandlerMethod = async (req, reply) => {
          for (const route of routesByMethod) {
            if (
              s3Router.matchRoute(route, {
                query: (req.query as Record<string, string>) || {},
                headers: (req.headers as Record<string, string>) || {},
              })
            ) {
              if (!route.handler) {
                throw new Error('no handler found')
              }

              try {
                req.operation = { type: route.operation }

                if (req.operation.type) {
                  trace.getActiveSpan()?.setAttribute('http.operation', req.operation.type)
                }

                const data: RequestInput<any> = {
                  Params: req.params,
                  Body: req.body,
                  Headers: req.headers,
                  Querystring: req.query,
                }
                const compiler = route.compiledSchema()
                const isValid = compiler(data)

                if (!isValid) {
                  throw { validation: compiler.errors }
                }

                const output = await route.handler(data, {
                  req: req,
                  storage: req.storage,
                  tenantId: req.tenantId,
                  owner: req.owner,
                  signals: {
                    body: req.signals.body.signal,
                    response: req.signals.response.signal,
                  },
                })

                const headers = output.headers

                if (headers) {
                  Object.keys(headers).forEach((header) => {
                    if (headers[header]) {
                      reply.header(header, headers[header])
                    }
                  })
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

          if (disableContentParser) {
            localFastify.addContentTypeParser(
              ['application/json', 'text/plain', 'application/xml'],
              function (request, payload, done) {
                done(null)
              }
            )
          }

          fastify.register(jsonToXml, {
            disableContentParser,
            parseAsArray: findArrayPathsInSchemas(
              routesByMethod.filter((r) => r.schema.Body).map((r) => r.schema.Body as JSONSchema)
            ),
          })
          fastify.register(signatureV4)
          fastify.register(db)
          fastify.register(storage)

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
