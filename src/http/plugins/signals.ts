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

      // Client terminated the request before the body was fully sent
      req.raw.once('close', () => {
        if (req.raw.aborted) {
          req.signals.body.abort()

          if (!req.signals.disconnect.signal.aborted) {
            req.signals.disconnect.abort()
          }
        }
      })

      // Client terminated the request before server finished sending the response
      res.raw.once('close', () => {
        const aborted = !res.raw.writableFinished
        if (aborted) {
          req.signals.response.abort()

          if (!req.signals.disconnect.signal.aborted) {
            req.signals.disconnect.abort()
          }
        }
      })
    })

    fastify.addHook('onRequestAbort', async (req) => {
      req.signals.body.abort()

      if (!req.signals.disconnect.signal.aborted) {
        req.signals.disconnect.abort()
      }
    })
  },
  { name: 'request-signals' }
)
