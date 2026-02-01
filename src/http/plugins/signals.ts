import fastifyPlugin from 'fastify-plugin'
import { FastifyInstance } from 'fastify'

declare module 'fastify' {
  interface FastifyRequest {
    signals: {
      body: AbortController
      response: AbortController
      disconnect: AbortController
    }
  }
}

const abortOnce = (ac: AbortController) => {
  if (!ac.signal.aborted) ac.abort()
}

export const signals = fastifyPlugin(
  async function (fastify: FastifyInstance) {
    fastify.addHook('onRequest', async (req, reply) => {
      req.signals = {
        body: new AbortController(),
        response: new AbortController(),
        disconnect: new AbortController(),
      }

      // Body upload interrupted (fires early)
      req.raw.once('close', () => {
        if (req.raw.aborted) {
          abortOnce(req.signals.body)
          abortOnce(req.signals.disconnect)
        }
      })

      // Response interrupted (connection closed before finish)
      reply.raw.once('close', () => {
        if (!reply.raw.writableFinished) {
          abortOnce(req.signals.response)
          abortOnce(req.signals.disconnect)
        }
      })
    })

    fastify.addHook('onRequestAbort', async (req) => {
      abortOnce(req.signals.body)
      abortOnce(req.signals.disconnect)
    })
  },
  { name: 'request-signals' }
)
