import { FastifyInstance, RouteHandlerMethod } from 'fastify'
import { db, jsonToXml, signatureV4, storage } from '../../plugins'
import { getRouter, RequestInput } from './router'
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
              })

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
            validatorCompiler: () => () => true,
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
              validatorCompiler: () => () => true,
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
