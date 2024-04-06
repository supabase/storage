import { FastifyInstance, RouteHandlerMethod } from 'fastify'
import { db, jsonToXml, signatureV4, storage } from '../../plugins'
import { getRouter } from './router'
import { s3ErrorHandler } from './error-handler'

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async (fastify) => {
    fastify.register(jsonToXml)
    fastify.register(signatureV4)
    fastify.register(db)
    fastify.register(storage)

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

        const schemaTypes = routesByMethod.map((r) => r.schema)
        const schema = schemaTypes.reduce(
          (acc, curr) => {
            if (curr.summary) {
              if (acc.summary) {
                acc.summary = `${acc.summary} | ${curr.summary}`
              } else {
                acc.summary = `${curr.summary}`
              }
            }
            if (curr.Params) {
              acc.params = {
                ...acc.params,
                ...(curr.Params as any),
              }
            }

            if (curr.Querystring) {
              acc.querystring = {
                ...acc.querystring,
                anyOf: [...(acc.querystring?.anyOf ? acc.querystring.anyOf : []), curr.Querystring],
              }
            }

            if (curr.Headers) {
              acc.headers = {
                ...acc.headers,
                anyOf: [...(acc.headers?.anyOf ? acc.headers.anyOf : []), curr.Headers],
              }
            }

            if (curr.Body && ['put', 'post', 'patch'].includes(method)) {
              acc.body = {
                ...acc.body,
                anyOf: [...(acc.body?.oneOf ? acc.body.oneOf : []), curr.Body],
              }
            }

            return acc
          },
          {
            tags: ['s3'],
          } as any
        )

        const routeHandler: RouteHandlerMethod = async (req, reply) => {
          for (const route of routesByMethod) {
            if (
              s3Router.matchRoute(route, {
                query: (req.query as any) || {},
                headers: (req.headers as any) || {},
              })
            ) {
              if (!route.handler) {
                throw new Error('no handler found')
              }
              const output = await route.handler(
                {
                  Params: req.params as any,
                  Body: req.body as any,
                  Headers: req.headers as any,
                  Querystring: req.query as any,
                  raw: req as any,
                },
                {
                  storage: req.storage,
                  tenantId: req.tenantId,
                  owner: req.owner,
                }
              )

              const headers = output.headers

              if (headers) {
                Object.keys(headers).forEach((header) => {
                  reply.header(header, headers[header])
                })
              }
              return reply.status(output.statusCode || 200).send(output.responseBody)
            }
          }

          return reply.status(404).send()
        }

        fastify[method](
          routePath,
          {
            schema,
            exposeHeadRoute: false,
            errorHandler: s3ErrorHandler,
          },
          routeHandler
        )

        // handle optional trailing slash
        if (!routePath.endsWith('*') && !routePath.endsWith('/')) {
          fastify[method](
            routePath + '/',
            {
              schema,
              exposeHeadRoute: false,
              errorHandler: s3ErrorHandler,
            },
            routeHandler
          )
        }
      })
    })
  })
}
