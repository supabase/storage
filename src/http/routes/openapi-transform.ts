import { SwaggerTransformObject } from '@fastify/swagger'
import { FastifySchema, RouteOptions } from 'fastify'

/**
 * @fastify/swagger names every de-duplicated component schema `def-0`, `def-1`, ... by
 * default, even for schemas registered with a meaningful `$id` (bucketSchema, errorSchema).
 * Use the `$id` as the component name instead, falling back to the default `def-N` for
 * anonymous schemas so unrelated inline schemas don't collide.
 */
export function nameSchemaByDollarId(
  json: { $id?: string },
  _baseUri: unknown,
  _fragment: unknown,
  i: number
) {
  return json.$id || `def-${i}`
}

const WILDCARD_PARAM = '*'
const WILDCARD_DOC_NAME = 'wildcard'

/**
 * Fastify's catch-all route segment is `*`, and its request params are keyed by the
 * literal `*` character (`request.params['*']`). @fastify/swagger mirrors that straight
 * into the OpenAPI doc as a path template `{*}` with a parameter named `*`, which isn't a
 * legal parameter/identifier name for any code generator. Rewrite it to a readable name for
 * docs only - the raw url string returned here still goes through Fastify's own `:name`
 * path-param formatting, and `route.schema` (the live validation schema) is never mutated.
 */
function renameWildcardParam(
  schema: FastifySchema,
  url: string
): { schema: FastifySchema; url: string } {
  if (!url.split('/').includes(WILDCARD_PARAM)) {
    return { schema, url }
  }

  const renamedUrl = url
    .split('/')
    .map((segment) => (segment === WILDCARD_PARAM ? `:${WILDCARD_DOC_NAME}` : segment))
    .join('/')

  const params = schema.params as
    | { properties?: Record<string, unknown>; required?: string[] }
    | undefined
  if (!params?.properties?.[WILDCARD_PARAM]) {
    return { schema, url: renamedUrl }
  }

  const { [WILDCARD_PARAM]: wildcardProperty, ...otherProperties } = params.properties
  const renamedParams = {
    ...params,
    properties: { ...otherProperties, [WILDCARD_DOC_NAME]: wildcardProperty },
    required: params.required?.map((name) => (name === WILDCARD_PARAM ? WILDCARD_DOC_NAME : name)),
  }

  return { schema: { ...schema, params: renamedParams }, url: renamedUrl }
}

/**
 * The S3-compatible surface dispatches ~18 real commands (PutObject, ListObjects,
 * CreateMultipartUpload, ...) from a handful of generic Fastify routes based on query
 * string/header matching done entirely inside the internal `s3/router.ts` Router - see
 * `s3/index.ts`. Fastify (and therefore @fastify/swagger) only ever sees the outer
 * catch-all route with no request/response schema, since OpenAPI has no way to express
 * "N distinct operations, same path and method, picked by a query parameter". Documenting
 * that catch-all as-is would give SDK generators a single method with an untyped body and
 * an untyped response for a request that's actually the whole S3 API - worse than nothing.
 * Hide it instead; the real per-command schemas remain the source of truth in s3/commands/*.
 */
function isS3ProtocolCatchAll(schema: FastifySchema | undefined, route: RouteOptions): boolean {
  const operation = (route.config as { operation?: string } | undefined)?.operation
  return (schema?.tags?.includes('s3') ?? false) && !operation
}

/**
 * Derives a stable, unique operationId from the route's `config.operation`
 * (see ROUTE_OPERATIONS in ./operations.ts), e.g. `storage.object.get_public` -> `objectGetPublic`.
 * Routes without a `config.operation` (protocol-level catch-alls like /s3 and /upload/resumable)
 * are left without an operationId.
 */
function operationToId(operation: string): string {
  const parts = operation.split('.').filter((part) => part !== 'storage')
  return parts
    .map((part) =>
      part
        .split('_')
        .filter(Boolean)
        .map((word, i) => (i === 0 ? word : word[0].toUpperCase() + word.slice(1)))
        .join('')
    )
    .map((part, i) => (i === 0 ? part : part[0].toUpperCase() + part.slice(1)))
    .join('')
}

const NON_STANDARD_ERROR_SHAPE_PATH_PREFIX = '/iceberg'

/**
 * Every route can end up hitting setErrorHandler and getting back a {statusCode, error,
 * message, code} body - default the doc to that shape for any otherwise-undocumented 4xx.
 * Doc-only on purpose: several handlers reply with an ad-hoc, partial error body directly
 * (`reply.status(400).send({message: '...'})`, bypassing the formatter entirely), and an
 * earlier version of this defaulted via a real onRoute hook that made Fastify enforce
 * errorSchema's `required` fields during response *serialization* - which threw on exactly
 * those ad-hoc replies (fast-json-stringify errors on a missing required property rather
 * than dropping it). A transform can't affect request handling, so it can't cause that.
 * Skipped entirely for the iceberg subtree: its `setErrorHandler` formatter
 * (src/http/routes/iceberg/index.ts) returns `{ error: { message, type, code } }`, not
 * errorSchema's flat `{statusCode, error, message, code}` - defaulting to errorSchema there
 * would document a shape iceberg never actually sends. Detected by path prefix rather than
 * `schema.tags`/`config.operation`, since some iceberg routes (src/http/routes/iceberg/bucket.ts)
 * reuse the same tag/operation constants as the unrelated storage-bucket routes. Leaves iceberg
 * 4xx responses undocumented for now - real documentation needs its own schema, tracked as
 * follow-up work alongside error-handler.ts's formatter-doc pairing.
 */
function defaultErrorResponse(schema: FastifySchema | undefined, url: string): FastifySchema {
  if (url.startsWith(NON_STANDARD_ERROR_SHAPE_PATH_PREFIX)) {
    return schema ?? {}
  }

  const response = schema?.response as Record<string, unknown> | undefined
  if (schema && response && Object.keys(response).some((status) => /^4xx$/i.test(status))) {
    return schema
  }

  return {
    ...schema,
    response: {
      ...(response ? undefined : { 200: { description: 'Default Response' } }),
      '4xx': { description: 'Error response', $ref: 'errorSchema#' },
      ...response,
    },
  }
}

/**
 * OpenAPI requires operationId to be unique across the whole document. A route can set
 * `config.operationId` to pin its id explicitly (takes precedence over the derived
 * `config.operation` id) - do this for any route whose id must stay stable regardless of
 * where it's registered, since generated SDK method names key off of it.
 * `exposeHeadRoutes` auto-derives a HEAD operation from every GET route re-using the same
 * `config.operation`/`config.operationId` - give that specific, deterministic case a `Head`
 * suffix. Any other collision (two distinct routes resolving to the same id) means the
 * route needs its own `config.operationId` - several pre-existing route families (tus,
 * object) already reuse the same ROUTE_OPERATIONS constant across multiple registrations
 * (e.g. POST / and POST /*), so this can't hard-fail doc generation for the whole app over
 * a pre-existing duplicate it doesn't own. Warn and leave the colliding route without an
 * operationId instead - no worse than before this transform existed, and each occurrence
 * is a route family that should get its own config.operationId in a follow-up.
 * Returns a fresh transform bound to its own dedup state, so main/admin specs don't
 * leak collisions into each other when generated in the same process (see export-docs.ts).
 */
export function createOpenApiTransform() {
  const seenIds = new Set<string>()

  return function transformOpenApiSchema({
    schema,
    url,
    route,
  }: {
    schema: FastifySchema
    url: string
    route: RouteOptions
  }): { schema: FastifySchema; url: string } {
    if (isS3ProtocolCatchAll(schema, route)) {
      return { schema: { ...schema, hide: true }, url }
    }

    ;({ schema, url } = renameWildcardParam(schema, url))
    schema = defaultErrorResponse(schema, url)

    const baseId =
      route.config?.operationId ??
      (route.config?.operation && operationToId(route.config.operation))

    if (!baseId || (schema as { operationId?: string }).operationId) {
      return { schema, url }
    }

    const methods = Array.isArray(route.method) ? route.method : [route.method]
    const isAutoHeadRoute = methods.length === 1 && methods[0] === 'HEAD'
    const operationId = isAutoHeadRoute ? `${baseId}Head` : baseId

    if (seenIds.has(operationId)) {
      console.warn(
        `[openapi] Duplicate operationId "${operationId}" for ${methods.join(',')} ${url} - ` +
          `leaving it undocumented. Give this route (or its ROUTE_OPERATIONS entry) a ` +
          `distinct config.operationId to fix.`
      )
      return { schema, url }
    }
    seenIds.add(operationId)

    return {
      schema: { ...schema, operationId },
      url,
    }
  }
}

/**
 * A handful of paths are reachable both with and without a trailing slash - either because
 * Fastify's own `exposeHeadRoutes` derives a HEAD route from the un-prefixed path (e.g.
 * /health vs /health/) or because a route is explicitly registered both ways (the S3
 * protocol surface). Both forms genuinely work at runtime, but documenting them as two
 * unrelated paths just doubles the number of operations a generated SDK has to deal with
 * for the same endpoint. Keep the slash-less form and fold the other one's methods into it.
 */
export const dedupeTrailingSlashPaths: SwaggerTransformObject = (documentObject) => {
  if (!('openapiObject' in documentObject)) {
    return documentObject.swaggerObject
  }

  const { openapiObject } = documentObject
  const paths = openapiObject.paths as Record<string, Record<string, unknown>> | undefined
  if (!paths) {
    return openapiObject
  }

  for (const url of Object.keys(paths)) {
    if (url === '/' || !url.endsWith('/')) {
      continue
    }

    const canonicalUrl = url.slice(0, -1)
    const canonicalPathItem = paths[canonicalUrl]
    if (!canonicalPathItem) {
      continue
    }

    for (const [method, operation] of Object.entries(paths[url])) {
      canonicalPathItem[method] ??= operation
    }
    delete paths[url]
  }

  return openapiObject
}
