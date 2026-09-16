import type { IncomingMessage, ServerResponse } from 'node:http'
import type { Http2ServerResponse } from 'node:http2'
import { StorageBackendError } from '@internal/errors'
import fastifyPlugin from 'fastify-plugin'

// Register once at the app root so child error handlers and direct replies share the hook.
export const closeConnectionOnError = fastifyPlugin(
  async function (fastify) {
    fastify.addHook('onSend', (request, reply, payload, done) => {
      if (reply.statusCode >= 400) {
        closeConnectionAfterResponse(request.raw, reply.raw, request.executionError)
      }
      done(null, payload)
    })
  },
  { name: 'close-connection-on-error' }
)

const closingResponses = new WeakSet<ServerResponse | Http2ServerResponse>()

// Called before headers are written by Fastify's onSend or the TUS writeHead wrapper.
// Close after the response so an unread body cannot stall a pooled connection.
export function closeConnectionAfterResponse(
  request: IncomingMessage,
  response: ServerResponse | Http2ServerResponse,
  error?: unknown
) {
  const unreadDeclaredBody =
    (request.headers['transfer-encoding'] !== undefined ||
      Number(request.headers['content-length']) > 0) &&
    !request.complete
  const explicitClose = error instanceof StorageBackendError && error.shouldCloseConnection()
  if (!unreadDeclaredBody && !explicitClose) return

  const socket = response.socket ?? request.socket
  if (socket.destroyed || closingResponses.has(response)) return
  closingResponses.add(response)

  response.setHeader('Connection', 'close')

  let timer: NodeJS.Timeout | undefined
  const cleanup = () => {
    clearTimeout(timer)
    response.off('finish', endConnection)
  }
  const endConnection = () => {
    if (socket.destroyed) return
    timer = setTimeout(() => {
      socket.off('close', cleanup)
      socket.destroy()
    }, 3000).unref()
    socket.end()
  }

  socket.once('close', cleanup)
  response.once('finish', endConnection)
}
