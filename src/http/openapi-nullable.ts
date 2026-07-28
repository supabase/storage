import type { SwaggerTransform } from '@fastify/swagger'
import { finiteSwaggerTransform } from './finite'

// Route schemas are authored as plain JSON Schema, e.g. `type: ['integer', 'null']`
// or `{ anyOf: [<schema>, { type: 'null' }] }` for `$ref`-based unions — both of
// which AJV validates natively. OpenAPI 3.0's Schema Object can't represent
// either idiom: it only allows a single `type` string plus a separate
// `nullable: true` boolean. Rewrite both into that OpenAPI 3.0 shape for the
// generated docs; the schemas used for runtime validation are untouched (same
// clone-before-mutate approach as `stripFiniteKeyword`).
function isNullOnlySchema(value: unknown): value is { type: 'null' } {
  return (
    !!value &&
    typeof value === 'object' &&
    Object.keys(value).length === 1 &&
    (value as { type?: unknown }).type === 'null'
  )
}

export function normalizeNullableSchema<T>(value: T): T {
  if (Array.isArray(value)) {
    return value.map(normalizeNullableSchema) as T
  }

  if (!value || typeof value !== 'object') {
    return value
  }

  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) {
    return value
  }

  const object = value as Record<string, unknown>

  const anyOf = object.anyOf
  if (Array.isArray(anyOf) && anyOf.length === 2) {
    const nullIndex = anyOf.findIndex(isNullOnlySchema)
    const other = nullIndex === -1 ? undefined : anyOf[1 - nullIndex]
    if (other && typeof other === 'object') {
      const merged: Record<string, unknown> = {
        ...(normalizeNullableSchema(other) as Record<string, unknown>),
        nullable: true,
      }
      for (const [key, nestedValue] of Object.entries(object)) {
        if (key !== 'anyOf' && !(key in merged)) {
          merged[key] = normalizeNullableSchema(nestedValue)
        }
      }
      return merged as T
    }
  }

  const result: Record<string, unknown> = {}
  for (const [key, nestedValue] of Object.entries(object)) {
    if (key === 'type' && Array.isArray(nestedValue) && nestedValue.length === 2) {
      const nullIndex = nestedValue.indexOf('null')
      if (nullIndex !== -1) {
        result.type = nestedValue[1 - nullIndex]
        result.nullable = true
        continue
      }
    }
    result[key] = normalizeNullableSchema(nestedValue)
  }

  return result as T
}

// Composes with `finiteSwaggerTransform`: strip the internal `finite` keyword
// first, then normalize nullable idioms, so the generated docs schema is both
// free of internal-only keywords and OpenAPI 3.0-valid.
export const docsSwaggerTransform: SwaggerTransform = (params) => {
  const { schema } = finiteSwaggerTransform(params)
  return { schema: normalizeNullableSchema(schema), url: params.url }
}
