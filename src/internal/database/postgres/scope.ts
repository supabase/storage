export interface Scope {
  role: string
  jwt: string
  subject: string
  claims: string
  headers: string
  method: string
  path: string
  operation: string
  statementTimeoutMs?: number
  searchPath?: string
}

export interface ScopeStatement {
  text: string
  values: unknown[]
}

export function buildScopeStatement(scope: Scope): ScopeStatement {
  const setters = [
    "set_config('role', $1, true)",
    "set_config('request.jwt.claim.role', $2, true)",
    "set_config('request.jwt', $3, true)",
    "set_config('request.jwt.claim.sub', $4, true)",
    "set_config('request.jwt.claims', $5, true)",
    "set_config('request.headers', $6, true)",
    "set_config('request.method', $7, true)",
    "set_config('request.path', $8, true)",
    "set_config('storage.operation', $9, true)",
    "set_config('storage.allow_delete_query', 'true', true)",
  ]
  const values: unknown[] = [
    scope.role,
    scope.role,
    scope.jwt,
    scope.subject,
    scope.claims,
    scope.headers,
    scope.method,
    scope.path,
    scope.operation,
  ]

  if (scope.statementTimeoutMs) {
    values.push(`${scope.statementTimeoutMs}ms`)
    setters.push(`set_config('statement_timeout', $${values.length}, true)`)
  }

  if (scope.searchPath) {
    values.push(scope.searchPath)
    setters.push(`set_config('search_path', $${values.length}, true)`)
  }

  return {
    text: `
        SELECT
          ${setters.join(',\n          ')};
      `,
    values,
  }
}
