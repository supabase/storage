import { describe, expect, it } from 'vitest'
import { buildScopeStatement, type Scope } from './scope'

const baseScope: Scope = {
  role: 'authenticated',
  jwt: 'jwt',
  subject: 'user-id',
  claims: '{"role":"authenticated"}',
  headers: '{"x-client-info":"test"}',
  method: 'POST',
  path: '/object/bucket/name',
  operation: 'object.create',
}

describe('PostgreSQL scope statement', () => {
  it('builds the common request scope without optional transaction settings', () => {
    const statement = buildScopeStatement(baseScope)

    expect(statement.text).toContain("set_config('role', $1, true)")
    expect(statement.text).not.toContain("set_config('statement_timeout'")
    expect(statement.text).not.toContain("set_config('search_path'")
    expect(statement.values).toEqual([
      'authenticated',
      'authenticated',
      'jwt',
      'user-id',
      '{"role":"authenticated"}',
      '{"x-client-info":"test"}',
      'POST',
      '/object/bucket/name',
      'object.create',
    ])
  })

  it('keeps timeout and search path placeholder ordering stable', () => {
    const statement = buildScopeStatement({
      ...baseScope,
      statementTimeoutMs: 4321,
      searchPath: 'storage,public,extensions',
    })

    expect(statement.text).toContain("set_config('statement_timeout', $10, true)")
    expect(statement.text).toContain("set_config('search_path', $11, true)")
    expect(statement.values.slice(9)).toEqual(['4321ms', 'storage,public,extensions'])
  })

  it('uses placeholder ten when only search path is present', () => {
    const statement = buildScopeStatement({
      ...baseScope,
      searchPath: 'storage,public,extensions',
    })

    expect(statement.text).toContain("set_config('search_path', $10, true)")
    expect(statement.values[9]).toBe('storage,public,extensions')
  })
})
