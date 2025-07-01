import { FastifyInstance } from 'fastify'
import { FastifyError } from '@fastify/error'
import { DatabaseError } from 'pg'
import { ErrorCode, isRenderableError, StorageError } from '@internal/errors'

/**
 * The global error handler for all the uncaught exceptions within a request.
 * We try our best to display meaningful information to our users
 * and log any error that occurs
 * @param app
 * @param options
 */
export const setErrorHandler = (
  app: FastifyInstance,
  options?: {
    respectStatusCode?: boolean
    formatter?: (error: StorageError) => Record<string, any>
  }
) => {
  app.setErrorHandler<Error>(function (error, request, reply) {
    const formatter = options?.formatter || ((e) => e)
    // We assign the error received.
    // it will be logged in the request log plugin
    request.executionError = error

    // database error
    if (
      error instanceof DatabaseError &&
      [
        'Authentication error', // supavisor specific
        'Max client connections reached',
        'remaining connection slots are reserved for non-replication superuser connections',
        'no more connections allowed',
        'sorry, too many clients already',
        'server login has been failing, try again later',
      ].some((msg) => (error as DatabaseError).message.includes(msg))
    ) {
      return reply.status(429).send(
        formatter({
          statusCode: `429`,
          error: 'too_many_connections',
          code: ErrorCode.SlowDown,
          message: 'Too many connections issued to the database',
        })
      )
    }

    if (isRenderableError(error)) {
      const renderableError = error.render()
      const statusCode = options?.respectStatusCode
        ? parseInt(renderableError.statusCode, 10)
        : error.userStatusCode
        ? error.userStatusCode
        : renderableError.statusCode === '500'
        ? 500
        : 400

      if (renderableError.code === ErrorCode.AbortedTerminate) {
        reply.header('Connection', 'close')

        reply.raw.once('finish', () => {
          setTimeout(() => {
            if (!request.raw.closed) {
              request.raw.destroy()
            }
          }, 3000)
        })
      }

      return reply.status(statusCode).send(
        formatter({
          ...renderableError,
          error: error.error || renderableError.code,
        })
      )
    }

    // Fastify errors
    if ('statusCode' in error) {
      const err = error as FastifyError
      return reply.status(err.statusCode || 500).send(
        formatter({
          statusCode: `${err.statusCode}`,
          error: err.name,
          code: ErrorCode.InternalError,
          message: err.message,
        })
      )
    }

    return reply.status(500).send(
      formatter({
        statusCode: '500',
        error: 'Internal',
        message: 'Internal Server Error',
        code: ErrorCode.InternalError,
      })
    )
  })
}
