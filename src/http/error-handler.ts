import { FastifyInstance } from 'fastify'
import { isRenderableError } from '../storage'
import { FastifyError } from '@fastify/error'

/**
 * The global error handler for all the uncaught exceptions within a request.
 * We try our best to display meaningful information to our users
 * and log any error that occurs
 * @param app
 */
export const setErrorHandler = (app: FastifyInstance) => {
  app.setErrorHandler<Error>(function (error, request, reply) {
    // We assign the error received.
    // it will be logged in the request log plugin
    reply.executionError = error

    request.log.error({ error }, `request error | ${request.id}`)

    if (process.env.NODE_ENV !== 'production') {
      console.error(error)
    }

    if (isRenderableError(error)) {
      const renderableError = error.render()
      return reply.status(renderableError.statusCode === '500' ? 500 : 400).send(renderableError)
    }

    // Fastify errors
    if ('statusCode' in error) {
      const err = error as FastifyError
      return reply.status((error as any).statusCode || 500).send({
        statusCode: `${err.statusCode}`,
        error: err.name,
        message: err.message,
      })
    }

    reply.status(500).send({
      statusCode: '500',
      error: 'Internal',
      message: 'Internal Server Error',
    })
  })
}
