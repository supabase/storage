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

const VALID_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/

function quoteIdentifier(name: string): string {
  if (!VALID_IDENTIFIER.test(name)) {
    throw ERRORS.InvalidParameter(`Invalid metadata field name: ${name}`)
  }
  return name
}

function quoteString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`
}

function quoteValue(value: FilterPrimitive): string {
  if (typeof value === 'string') return quoteString(value)
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw ERRORS.InvalidParameter(`Filter values must be finite numbers, got: ${value}`)
    }
    return String(value)
  }
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  throw ERRORS.InvalidParameter(`Unsupported filter value type: ${typeof value}`)
}

function translateFieldOperator(field: string, op: string, raw: unknown): string {
  const f = quoteIdentifier(field)
  switch (op) {
    case '$eq':
      return `${f} = ${quoteValue(raw as FilterPrimitive)}`
    case '$ne':
      return `${f} != ${quoteValue(raw as FilterPrimitive)}`
    case '$gt':
      return `${f} > ${quoteValue(raw as number)}`
    case '$gte':
      return `${f} >= ${quoteValue(raw as number)}`
    case '$lt':
      return `${f} < ${quoteValue(raw as number)}`
    case '$lte':
      return `${f} <= ${quoteValue(raw as number)}`
    case '$in': {
      if (!Array.isArray(raw) || raw.length === 0) {
        throw ERRORS.InvalidParameter(`$in requires a non-empty array for "${field}"`)
      }
      return `${f} IN (${(raw as FilterPrimitive[]).map(quoteValue).join(', ')})`
    }
    case '$nin': {
      if (!Array.isArray(raw) || raw.length === 0) {
        throw ERRORS.InvalidParameter(`$nin requires a non-empty array for "${field}"`)
      }
      return `${f} NOT IN (${(raw as FilterPrimitive[]).map(quoteValue).join(', ')})`
    }
    case '$exists': {
      if (typeof raw !== 'boolean') {
        throw ERRORS.InvalidParameter(`$exists requires a boolean for "${field}"`)
      }
      return raw ? `${f} IS NOT NULL` : `${f} IS NULL`
    }
    default:
      throw ERRORS.InvalidParameter(`Unsupported field operator: ${op}`)
  }
}

function translateFieldClause(field: string, value: FilterPrimitive | FieldOperators): string {
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    const ops = Object.entries(value as FieldOperators)
    if (ops.length === 0) {
      throw ERRORS.InvalidParameter(`Empty operator object for field "${field}"`)
    }
    for (const [op] of ops) {
      if (!FIELD_OPERATORS.has(op)) {
        throw ERRORS.InvalidParameter(`Unsupported field operator: ${op}`)
      }
    }
    const parts = ops.map(([op, raw]) => translateFieldOperator(field, op, raw))
    return parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`
  }
  return `${quoteIdentifier(field)} = ${quoteValue(value as FilterPrimitive)}`
}

export function translateFilter(filter: S3VectorFilter): string {
  if (filter === null || typeof filter !== 'object' || Array.isArray(filter)) {
    throw ERRORS.InvalidParameter('Filter must be an object')
  }

  const keys = Object.keys(filter)
  if (keys.length === 0) {
    throw ERRORS.InvalidParameter('Filter must contain at least one clause')
  }

  if ('$and' in filter || '$or' in filter) {
    if (keys.length !== 1) {
      throw ERRORS.InvalidParameter(
        `Logical operator must be the only key, got: ${keys.join(', ')}`
      )
    }
    const op = keys[0] as '$and' | '$or'
    const sub = (filter as { $and?: S3VectorFilter[]; $or?: S3VectorFilter[] })[op]
    if (!Array.isArray(sub) || sub.length === 0) {
      throw ERRORS.InvalidParameter(`${op} requires a non-empty array`)
    }
    const joined = sub.map((s) => `(${translateFilter(s)})`).join(op === '$and' ? ' AND ' : ' OR ')
    return joined
  }

  const clauses = keys.map((field) => {
    if (field.startsWith('$')) {
      throw ERRORS.InvalidParameter(`Unexpected operator "${field}" at field position`)
    }
    return translateFieldClause(
      field,
      (filter as Record<string, FilterPrimitive | FieldOperators>)[field]
    )
  })
  return clauses.length === 1 ? clauses[0] : clauses.join(' AND ')
}
