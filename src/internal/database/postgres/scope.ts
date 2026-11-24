import { ERRORS } from '@internal/errors'
import { getConfig } from '../../../config'
import type { TenantConnectionOptions } from '../pool'

export type ScopeConnectionOptions = Pick<
  TenantConnectionOptions,
  'user' | 'method' | 'path' | 'operation'
>

export interface ScopeStatement {
  text: string
  values: unknown[]
}

const SCOPE_CONFIG_SETTERS = `set_config('role', $1, true),
          set_config('request.jwt.claim.role', $2, true),
          set_config('request.jwt', $3, true),
          set_config('request.jwt.claim.sub', $4, true),
          set_config('request.jwt.claims', $5, true),
          set_config('request.headers', $6, true),
          set_config('request.method', $7, true),
          set_config('request.path', $8, true),
          set_config('storage.operation', $9, true),
          set_config('storage.allow_delete_query', 'true', true)`

const SCOPE_SETTERS_WITH_STATEMENT_TIMEOUT = `${SCOPE_CONFIG_SETTERS},
          set_config('statement_timeout', $10, true)`

const SCOPE_SETTERS_WITH_SEARCH_PATH = `${SCOPE_CONFIG_SETTERS},
          set_config('search_path', $10, true)`

const SCOPE_SETTERS_WITH_STATEMENT_TIMEOUT_AND_SEARCH_PATH = `${SCOPE_CONFIG_SETTERS},
          set_config('statement_timeout', $10, true),
          set_config('search_path', $11, true)`

interface ScopeConfigSql {
  postgres: string
  multigres: string
}

// set_config returns the value it sets, so selecting the setters themselves sends the JWT, its
// claims and the request headers back on every transaction. On Postgres the statement selects no
// columns from them instead, which returns an empty row. Postgres still runs every setter: it never
// drops a subquery output that calls a volatile function, and set_config is volatile. Multigres
// only accepts set_config as a top-level SELECT target, so there the setters are selected directly.
function scopeConfigSql(setters: string): ScopeConfigSql {
  return {
    postgres: `
        SELECT FROM (SELECT
          ${setters}) AS scope;
      `,
    multigres: `
        SELECT
          ${setters};
      `,
  }
}

const SCOPE_CONFIG_SQL = scopeConfigSql(SCOPE_CONFIG_SETTERS)
const SCOPE_CONFIG_SQL_WITH_STATEMENT_TIMEOUT = scopeConfigSql(SCOPE_SETTERS_WITH_STATEMENT_TIMEOUT)
const SCOPE_CONFIG_SQL_WITH_SEARCH_PATH = scopeConfigSql(SCOPE_SETTERS_WITH_SEARCH_PATH)
const SCOPE_CONFIG_SQL_WITH_STATEMENT_TIMEOUT_AND_SEARCH_PATH = scopeConfigSql(
  SCOPE_SETTERS_WITH_STATEMENT_TIMEOUT_AND_SEARCH_PATH
)

export function buildScopeStatement(
  options: ScopeConnectionOptions,
  role: string,
  claimsPayload: string,
  headersPayload: string,
  statementTimeoutMs?: number,
  searchPath?: string
): ScopeStatement {
  // PostgreSQL treats `none` as a reset to the session user which can own
  // Storage tables and bypass their RLS. It must never be a caller role.
  if (role === 'none') {
    throw ERRORS.AccessDenied('Invalid role').withStatusCode(403)
  }

  const { isMultitenant, dbSuperUser, dbRejectSuperUserRole, databaseEngine } = getConfig()
  if (isMultitenant && dbRejectSuperUserRole && role === dbSuperUser) {
    throw ERRORS.AccessDenied('Invalid role').withStatusCode(403)
  }

  const values: unknown[] = [
    role,
    role,
    options.user.jwt || '',
    options.user.payload.sub || '',
    claimsPayload,
    headersPayload,
    options.method || '',
    options.path || '',
    options.operation?.() || '',
  ]

  if (statementTimeoutMs) {
    values.push(`${statementTimeoutMs}ms`)
  }

  if (searchPath) {
    values.push(searchPath)
  }

  const sql = getScopeConfigSql(statementTimeoutMs, searchPath)
  return {
    text: databaseEngine === 'multigres' ? sql.multigres : sql.postgres,
    values,
  }
}

function getScopeConfigSql(
  statementTimeoutMs: number | undefined,
  searchPath: string | undefined
): ScopeConfigSql {
  if (statementTimeoutMs && searchPath) {
    return SCOPE_CONFIG_SQL_WITH_STATEMENT_TIMEOUT_AND_SEARCH_PATH
  }

  if (statementTimeoutMs) {
    return SCOPE_CONFIG_SQL_WITH_STATEMENT_TIMEOUT
  }

  if (searchPath) {
    return SCOPE_CONFIG_SQL_WITH_SEARCH_PATH
  }

  return SCOPE_CONFIG_SQL
}
