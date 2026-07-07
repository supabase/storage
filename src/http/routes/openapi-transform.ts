import { FastifySchema, RouteOptions } from 'fastify'

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
    const operation = (route.config as { operation?: string } | undefined)?.operation

    if (!operation || (schema as { operationId?: string }).operationId) {
      return { schema, url }
    }

    let operationId = operationToId(operation)
    if (seenIds.has(operationId)) {
      const method = Array.isArray(route.method) ? route.method[0] : route.method
      operationId += method[0].toUpperCase() + method.slice(1).toLowerCase()
    }
    seenIds.add(operationId)

    return {
      schema: { ...schema, operationId },
      url,
    }
  }
}
