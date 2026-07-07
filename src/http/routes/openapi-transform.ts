import { SwaggerTransformObject } from '@fastify/swagger'
import { FastifySchema, RouteOptions } from 'fastify'

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

/**
 * OpenAPI requires operationId to be unique across the whole document. `exposeHeadRoutes`
 * auto-derives a HEAD operation from every GET route re-using the same `config.operation`,
 * so the id needs a per-method suffix to stay unique when that happens.
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
    ;({ schema, url } = renameWildcardParam(schema, url))

    const operation = (route.config as { operation?: string } | undefined)?.operation

    if (!operation || (schema as { operationId?: string }).operationId) {
      return { schema, url }
    }

    const baseId = operationToId(operation)
    let operationId = baseId
    if (seenIds.has(operationId)) {
      const method = Array.isArray(route.method) ? route.method[0] : route.method
      operationId = baseId + method[0].toUpperCase() + method.slice(1).toLowerCase()
    }
    for (let suffix = 2; seenIds.has(operationId); suffix++) {
      operationId = baseId + suffix
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
