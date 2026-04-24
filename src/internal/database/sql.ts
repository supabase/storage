const POSTGRES_IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/

export function quoteIdentifier(identifier: string): string {
  if (!POSTGRES_IDENTIFIER_PATTERN.test(identifier)) {
    throw new Error(`Invalid PostgreSQL identifier: ${identifier}`)
  }

  return `"${identifier}"`
}

export function quoteQualifiedIdentifier(tableName: string): string {
  const [schema, name, ...rest] = tableName.split('.')

  if (!schema || !name || rest.length > 0) {
    throw new Error(`Invalid PostgreSQL table name: ${tableName}`)
  }

  return `${quoteIdentifier(schema)}.${quoteIdentifier(name)}`
}
