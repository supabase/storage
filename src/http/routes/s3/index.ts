import { FastifyInstance, RouteHandlerMethod } from 'fastify'
import { db, jsonToXml, signatureV4, storage } from '../../plugins'
import { getRouter } from './router'
import { FastifyError } from '@fastify/error'
import { FastifyRequest } from 'fastify/types/request'
import { FastifyReply } from 'fastify/types/reply'
import { ErrorCode, StorageBackendError } from '../../../storage'
import { DatabaseError } from 'pg'

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async (fastify) => {
    fastify.register(signatureV4)
    fastify.register(jsonToXml)
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

        const errorHandler = (
          error: FastifyError | Error,
          request: FastifyRequest,
          reply: FastifyReply
        ) => {
          request.executionError = error

          const resource = request.url
            .split('?')[0]
            .replace('/s3', '')
            .split('/')
            .filter((e) => e)
            .join('/')

          // database error
          if (
            error instanceof DatabaseError &&
            [
              'Max client connections reached',
              'remaining connection slots are reserved for non-replication superuser connections',
              'no more connections allowed',
              'sorry, too many clients already',
              'server login has been failing, try again later',
            ].some((msg) => (error as DatabaseError).message.includes(msg))
          ) {
            return reply.status(429).send({
              Error: {
                Resource: resource,
                Code: ErrorCode.SlowDown,
                Message: 'Too many connections issued to the database',
              },
            })
          }

          if (error instanceof StorageBackendError) {
            return reply.status(error.httpStatusCode || 500).send({
              Error: {
                Resource: resource,
                Code: error.code,
                Message: error.message,
              },
            })
          }

          return reply.status(500).send({
            Error: {
              Code: 'Internal',
              Message: 'Internal Server Error',
            },
          })
        }

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
            errorHandler: errorHandler,
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
              errorHandler: errorHandler,
            },
            routeHandler
          )
        }
      })
    })
  })
}
