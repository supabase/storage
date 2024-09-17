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

export const signals = fastifyPlugin(
  async function (fastify: FastifyInstance) {
    fastify.addHook('onRequest', async (req, res) => {
      req.signals = {
        body: new AbortController(),
        response: new AbortController(),
        disconnect: new AbortController(),
      }

      // Client terminated the request before the body was fully received
      res.raw.once('close', () => {
        req.signals.response.abort()

        if (!req.signals.disconnect.signal.aborted) {
          req.signals.disconnect.abort()
        }
      })
    })

    // Client terminated the request before the body was fully sent
    fastify.addHook('onRequestAbort', async (req) => {
      req.signals.body.abort()

      if (!req.signals.disconnect.signal.aborted) {
        req.signals.disconnect.abort()
      }
    })
  },
  { name: 'request-signals' }
)
