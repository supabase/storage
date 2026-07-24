import { FastifyError } from '@fastify/error'
import {
  ErrorCode,
  getErrorCode,
  isRenderableError,
  StorageBackendError,
  StorageError,
} from '@internal/errors'
import { isDatabaseSlowDownError } from '@internal/errors/database-error'
import { FastifyInstance } from 'fastify'

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
    formatter?: (error: StorageError) => Record<string, unknown>
    // JSON schema fragment matching what `formatter` actually produces - required alongside
    // `formatter` to keep the doc default (below) truthful for a subtree with a reshaped
    // error body, e.g. iceberg/index.ts's REST-catalog-spec {error: {message, type, code}}.
    errorResponseSchema?: object
  }
) => {
  // registerJwtAuth's onRoute hook documents a flat {statusCode, error, message, code} 403 on
  // every route it guards, since that's what a real AccessDenied error always renders as - true
  // for every subtree except one with a custom `formatter` here that reshapes the wire response
  // entirely (e.g. iceberg/index.ts's REST-catalog-spec {error: {message, type, code}}). Without
  // this override, that subtree's routes would keep documenting (and, worse, get response-
  // schema-serialized against) the wrong flat shape: fast-json-stringify chokes on the type
  // mismatch (error typed as a string in errorSchema, sent as an object) and 500s. This hook
  // runs after registerJwtAuth's (added earlier in every caller of this file) so it overrides
  // rather than only filling a gap.
  //
  // There's deliberately no equivalent generic default for the plain (no formatter) case: this
  // codebase has many handlers that reply with an ad-hoc, partial error body directly
  // (`reply.status(400).send({message: '...'})`, bypassing this file's formatter entirely), and
  // forcing errorSchema's `required` fields onto those routes' response serialization makes
  // fast-json-stringify throw on the ones actually missing a field - a real, previously-hit
  // regression (see git history). The generic "errorSchema is probably the shape of a 4xx"
  // default lives in openapi-transform.ts's doc-only transform instead, which can't affect
  // real request handling either way.
  if (options?.formatter && options.errorResponseSchema) {
    app.addHook('onRoute', (routeOptions) => {
      routeOptions.schema = routeOptions.schema || {}
      const hadResponseSchema = Boolean(routeOptions.schema.response)
      const existingResponse = { ...(routeOptions.schema.response as object | undefined) }
      // Drop any status-specific entry an ancestor already added (registerJwtAuth's 403,
      // a shallower setErrorHandler's 4xx) - all of them assumed the flat errorSchema shape,
      // which this subtree's formatter doesn't produce, so none of them are still accurate.
      for (const status of Object.keys(existingResponse)) {
        if (status !== '200') {
          delete (existingResponse as Record<string, unknown>)[status]
        }
      }
      routeOptions.schema.response = {
        ...(hadResponseSchema ? undefined : { 200: { description: 'Default Response' } }),
        ...existingResponse,
        '4xx': { description: 'Error response', ...options.errorResponseSchema },
      }
    })
  }

  app.setErrorHandler<Error>(function (error, request, reply) {
    const formatter = options?.formatter || ((e) => e)
    // We assign the error received.
    // it will be logged in the request log plugin
    request.executionError = error

    // database error
    if (isDatabaseSlowDownError(error)) {
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

      if (
        renderableError.code === ErrorCode.AbortedTerminate ||
        (error instanceof StorageBackendError && error.shouldCloseConnection())
      ) {
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

      if (err.code === 'FST_ERR_CTP_INVALID_MEDIA_TYPE') {
        return reply.status(400).send(
          formatter({
            statusCode: '415',
            code: ErrorCode.InvalidMimeType,
            error: 'invalid_mime_type',
            message: 'Invalid Content-Type header',
          })
        )
      }

      const errorCode = getErrorCode(err)
      const responseErrorCode = (
        errorCode === ErrorCode.UnknownError ? ErrorCode.InternalError : errorCode
      ) as ErrorCode
      const responseStatusCode = err.statusCode || 500

      return reply.status(responseStatusCode).send(
        formatter({
          statusCode: `${responseStatusCode}`,
          error: err.name,
          code: responseErrorCode,
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
