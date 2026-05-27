import { ERRORS } from '@internal/errors'

type FilterPrimitive = string | number | boolean

type FieldOperators = {
  $eq?: FilterPrimitive
  $ne?: FilterPrimitive
  $gt?: number
  $gte?: number
  $lt?: number
  $lte?: number
  $in?: FilterPrimitive[]
  $nin?: FilterPrimitive[]
  $exists?: boolean
}

export type S3VectorFilter =
  | { $and: S3VectorFilter[] }
  | { $or: S3VectorFilter[] }
  | { [fieldName: string]: FilterPrimitive | FieldOperators }

const FIELD_OPERATORS = new Set([
  '$eq',
  '$ne',
  '$gt',
  '$gte',
  '$lt',
  '$lte',
  '$in',
  '$nin',
  '$exists',
])

// Full SQL column reference: optional schema/table prefix segments followed by
// a final column name. Each segment must be a SQL identifier. Used to validate
// the `column` override end-to-end (the previous regex only checked the last
// segment, which would allow injection if the override were ever user-derived).
const VALID_COLUMN_REF = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/

export interface TranslatedFilter {
  sql: string
  params: unknown[]
}

interface Ctx {
  column: string
  params: unknown[]
}

function placeholder(ctx: Ctx, value: unknown): string {
  ctx.params.push(value)
  return `$${ctx.params.length}`
}

/**
 * Render a JSONB key access where the key itself is parameterized. This lets
 * us accept any metadata key (including hyphens, dots, spaces — whatever the
 * S3Vectors caller stored) without restricting key names to SQL-identifier
 * shape and without escaping issues.
 */
function jsonText(ctx: Ctx, fieldName: string): string {
  return `${ctx.column}->>${placeholder(ctx, fieldName)}`
}

function jsonValue(ctx: Ctx, fieldName: string): string {
  return `${ctx.column}->${placeholder(ctx, fieldName)}`
}

function checkFinite(value: number): void {
  if (!Number.isFinite(value)) {
    throw ERRORS.InvalidParameter('filter', {
      message: `Filter values must be finite numbers, got: ${value}`,
    })
  }
}

function validatePrimitive(value: unknown): FilterPrimitive {
  if (typeof value === 'string') return value
  if (typeof value === 'number') {
    checkFinite(value)
    return value
  }
  if (typeof value === 'boolean') return value
  throw ERRORS.InvalidParameter('filter', {
    message: `Unsupported filter value type: ${typeof value}`,
  })
}

function primitiveAsText(value: FilterPrimitive): string {
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  return String(value)
}

function translateFieldOperator(ctx: Ctx, fieldName: string, op: string, raw: unknown): string {
  switch (op) {
    case '$eq': {
      const v = validatePrimitive(raw)
      const lhs = jsonText(ctx, fieldName)
      return `${lhs} = ${placeholder(ctx, primitiveAsText(v))}`
    }
    case '$ne': {
      const v = validatePrimitive(raw)
      const lhs = jsonText(ctx, fieldName)
      return `${lhs} <> ${placeholder(ctx, primitiveAsText(v))}`
    }
    case '$gt':
    case '$gte':
    case '$lt':
    case '$lte': {
      if (typeof raw !== 'number') {
        throw ERRORS.InvalidParameter('filter', {
          message: `${op} requires a number for "${fieldName}"`,
        })
      }
      checkFinite(raw)
      const opSql = { $gt: '>', $gte: '>=', $lt: '<', $lte: '<=' }[op]
      // Guard the numeric cast with a jsonb_typeof check so non-numeric values
      // are skipped instead of raising "invalid input syntax for type numeric".
      const valNode = jsonValue(ctx, fieldName)
      const txtNode = jsonText(ctx, fieldName)
      const cmpParam = placeholder(ctx, raw)
      return `(jsonb_typeof(${valNode}) = 'number' AND (${txtNode})::numeric ${opSql} ${cmpParam})`
    }
    case '$in': {
      if (!Array.isArray(raw) || raw.length === 0) {
        throw ERRORS.InvalidParameter('filter', {
          message: `$in requires a non-empty array for "${fieldName}"`,
        })
      }
      const values = (raw as FilterPrimitive[]).map((v) => primitiveAsText(validatePrimitive(v)))
      const lhs = jsonText(ctx, fieldName)
      return `${lhs} = ANY(${placeholder(ctx, values)})`
    }
    case '$nin': {
      if (!Array.isArray(raw) || raw.length === 0) {
        throw ERRORS.InvalidParameter('filter', {
          message: `$nin requires a non-empty array for "${fieldName}"`,
        })
      }
      const values = (raw as FilterPrimitive[]).map((v) => primitiveAsText(validatePrimitive(v)))
      const lhs = jsonText(ctx, fieldName)
      return `${lhs} <> ALL(${placeholder(ctx, values)})`
    }
    case '$exists': {
      if (typeof raw !== 'boolean') {
        throw ERRORS.InvalidParameter('filter', {
          message: `$exists requires a boolean for "${fieldName}"`,
        })
      }
      // Use the function form `jsonb_exists` instead of the `?` operator: the
      // bare `?` in SQL collides with knex's positional placeholder parser
      // when this fragment is later embedded in a knex.raw call.
      const keyParam = placeholder(ctx, fieldName)
      return raw
        ? `jsonb_exists(${ctx.column}, ${keyParam})`
        : `NOT jsonb_exists(${ctx.column}, ${keyParam})`
    }
    default:
      throw ERRORS.InvalidParameter('filter', {
        message: `Unsupported field operator: ${op}`,
      })
  }
}

function translateFieldClause(
  ctx: Ctx,
  fieldName: string,
  value: FilterPrimitive | FieldOperators
): string {
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    const ops = Object.entries(value as FieldOperators)
    if (ops.length === 0) {
      throw ERRORS.InvalidParameter('filter', {
        message: `Empty operator object for field "${fieldName}"`,
      })
    }
    for (const [op] of ops) {
      if (!FIELD_OPERATORS.has(op)) {
        throw ERRORS.InvalidParameter('filter', {
          message: `Unsupported field operator: ${op}`,
        })
      }
    }
    const parts = ops.map(([op, raw]) => translateFieldOperator(ctx, fieldName, op, raw))
    return parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`
  }
  const v = validatePrimitive(value)
  const lhs = jsonText(ctx, fieldName)
  return `${lhs} = ${placeholder(ctx, primitiveAsText(v))}`
}

function translateInternal(ctx: Ctx, filter: S3VectorFilter): string {
  if (filter === null || typeof filter !== 'object' || Array.isArray(filter)) {
    throw ERRORS.InvalidParameter('filter', { message: 'Filter must be an object' })
  }

  const keys = Object.keys(filter)
  if (keys.length === 0) {
    throw ERRORS.InvalidParameter('filter', {
      message: 'Filter must contain at least one clause',
    })
  }

  if ('$and' in filter || '$or' in filter) {
    if (keys.length !== 1) {
      throw ERRORS.InvalidParameter('filter', {
        message: `Logical operator must be the only key, got: ${keys.join(', ')}`,
      })
    }
    const op = keys[0] as '$and' | '$or'
    const sub = (filter as { $and?: S3VectorFilter[]; $or?: S3VectorFilter[] })[op]
    if (!Array.isArray(sub) || sub.length === 0) {
      throw ERRORS.InvalidParameter('filter', {
        message: `${op} requires a non-empty array`,
      })
    }
    const joiner = op === '$and' ? ' AND ' : ' OR '
    return sub.map((s) => `(${translateInternal(ctx, s)})`).join(joiner)
  }

  const clauses = keys.map((name) => {
    if (name.startsWith('$')) {
      throw ERRORS.InvalidParameter('filter', {
        message: `Unexpected operator "${name}" at field position`,
      })
    }
    return translateFieldClause(
      ctx,
      name,
      (filter as Record<string, FilterPrimitive | FieldOperators>)[name]
    )
  })
  return clauses.length === 1 ? clauses[0] : clauses.join(' AND ')
}

/**
 * Translate an S3Vectors-shape filter into a parameterized SQL fragment
 * targeting a JSONB metadata column.
 *
 * @param filter   parsed JSON filter object from the QueryVectors request
 * @param column   fully qualified JSONB column reference (e.g. `t.metadata`).
 *                 Validated against an identifier-segment regex end-to-end —
 *                 callers should only ever pass a value they control.
 * @returns        { sql, params } where `sql` uses $1, $2, … placeholders aligned with `params`
 */
export function translateFilter(filter: S3VectorFilter, column = 'metadata'): TranslatedFilter {
  if (!VALID_COLUMN_REF.test(column)) {
    throw ERRORS.InvalidParameter('column', {
      message: `Invalid metadata column reference: ${column}`,
    })
  }
  const ctx: Ctx = { column, params: [] }
  const sql = translateInternal(ctx, filter)
  return { sql, params: ctx.params }
}
