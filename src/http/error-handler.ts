import { FastifyInstance } from 'fastify'
import { isRenderableError } from '../storage'
import { FastifyError } from '@fastify/error'
import { DatabaseError } from 'pg'

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

    if (isRenderableError(error)) {
      const renderableError = error.render()
      const statusCode = error.userStatusCode
        ? error.userStatusCode
        : renderableError.error === '500'
        ? 500
        : 400
      return reply.status(statusCode).send(renderableError)
    }

    // database error
    if (
      error instanceof DatabaseError &&
      [
        'remaining connection slots are reserved for non-replication superuser connections',
        'no more connections allowed',
        'sorry, too many clients already',
        'server login has been failing, try again later',
      ].some((msg) => (error as DatabaseError).message.includes(msg))
    ) {
      return reply.status(429).send({
        statusCode: `429`,
        error: 'too_many_connections',
        message: 'Too many connections issued to the database',
      })
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
