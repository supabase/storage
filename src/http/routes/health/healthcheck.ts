import { FastifyInstance } from 'fastify'

export default async function routes(fastify: FastifyInstance) {
  const summary = 'healthcheck'

  fastify.get(
    '/',
    {
      schema: {
        operationId: 'healthcheck',
        summary,
        description:
          'Checks database connectivity and always responds with HTTP 200, reporting healthy: false in the body rather than an error status when the check fails',
        tags: ['health'],
      },
    },
    async (req, res) => {
      try {
        await req.storage.healthcheck()
        return res.send({ healthy: true })
      } catch (e) {
        if (e instanceof Error) {
          req.executionError = e
        }
        return res.send({ healthy: false })
      }
    }
  )
}
