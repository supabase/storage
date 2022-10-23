import { FastifyInstance } from 'fastify'
import { isRenderableError } from '../storage'
import { FastifyError } from '@fastify/error'

export const setErrorHandler = (app: FastifyInstance) => {
  app.setErrorHandler<Error>(function (error, request, reply) {
    if (isRenderableError(error)) {
      this.log.error({ error, originalError: error.getOriginalError() }, error.message)
      return reply.status(400).send(error.render())
    }

    this.log.error({ error }, error.message)

    // Fastify errors
    if ('statusCode' in error) {
      const err = error as FastifyError
      return reply.status((error as any).statusCode).send({
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
