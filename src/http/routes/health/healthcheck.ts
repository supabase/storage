import { FastifyInstance } from 'fastify'

export default async function routes(fastify: FastifyInstance) {
  const summary = 'healthcheck'

  fastify.get(
    '/',
    {
      schema: {
        summary,
      },
    },
    async (req, res) => {
      try {
        await req.storage.healthcheck()
        res.send({ healthy: true })
      } catch (e) {
        if (e instanceof Error) {
          req.executionError = e
        }
        res.send({ healthy: false })
      }
    }
  )
}
