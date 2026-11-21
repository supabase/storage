import { beforeEach, describe, expect, it, vi } from 'vitest'
import { buildScopeStatement, type ScopeConnectionOptions } from './scope'

const config = vi.hoisted(() => ({
  isMultitenant: false,
  dbRejectSuperUserRole: false,
  dbSuperUser: 'supabase_storage_admin',
}))
vi.mock('../../../config', () => ({ getConfig: () => config }))

const baseScopeOptions: ScopeConnectionOptions = {
  user: {
    jwt: 'jwt',
    payload: {
      role: 'authenticated',
      sub: 'user-id',
    },
  },
  method: 'POST',
  path: '/object/bucket/name',
  operation: () => 'object.create',
}
const role = 'authenticated'
const claims = '{"role":"authenticated"}'
const headers = '{"x-client-info":"test"}'

describe('PostgreSQL scope statement', () => {
  beforeEach(() => {
    config.isMultitenant = true
    config.dbRejectSuperUserRole = true
    config.dbSuperUser = 'supabase_storage_admin'
  })

  it.each([
    [true, true, 'supabase_storage_admin', 'supabase_storage_admin', true],
    [true, true, 'postgres', 'postgres', true],
    [true, true, 'custom_admin', 'custom_admin', true],
    [true, true, 'service_role', 'service_role', true],
    [true, false, 'service_role', 'service_role', false],
    [true, false, 'postgres', 'postgres', false],
    [false, true, 'postgres', 'postgres', false],
    [true, false, 'postgres', 'none', true],
  ] as const)(
    'multitenant=%s rejectAdmin=%s admin=%s role=%s rejects=%s',
    (isMultitenant, rejectAdmin, adminRole, role, rejects) => {
      Object.assign(config, {
        isMultitenant,
        dbRejectSuperUserRole: rejectAdmin,
        dbSuperUser: adminRole,
      })
      const build = () => buildScopeStatement(baseScopeOptions, role, claims, headers)
      if (rejects) {
        expect(build).toThrowError(
          expect.objectContaining({ code: 'AccessDenied', httpStatusCode: 403 })
        )
      } else {
        expect(build().values.slice(0, 2)).toEqual([role, role])
      }
    }
  )

  it('rejects the reserved role reset value', () => {
    expect(() =>
      buildScopeStatement(baseScopeOptions, 'none', '{"role":"none"}', headers)
    ).toThrowError(
      expect.objectContaining({
        code: 'AccessDenied',
        httpStatusCode: 403,
      })
    )
  })

  it.each(['anon', 'authenticated', 'service_role', 'custom_role', 'NONE'])(
    'preserves the database role %s',
    (role) => {
      const statement = buildScopeStatement(baseScopeOptions, role, claims, headers)
      expect(statement.values.slice(0, 2)).toEqual([role, role])
    }
  )

  it('builds the common request scope without optional transaction settings', () => {
    const statement = buildScopeStatement(baseScopeOptions, role, claims, headers)

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
    const statement = buildScopeStatement(
      baseScopeOptions,
      role,
      claims,
      headers,
      4321,
      'storage,public,extensions'
    )

    expect(statement.text).toContain("set_config('statement_timeout', $10, true)")
    expect(statement.text).toContain("set_config('search_path', $11, true)")
    expect(statement.values.slice(9)).toEqual(['4321ms', 'storage,public,extensions'])
  })

  it('uses placeholder ten when only search path is present', () => {
    const statement = buildScopeStatement(
      baseScopeOptions,
      role,
      claims,
      headers,
      undefined,
      'storage,public,extensions'
    )

    expect(statement.text).toContain("set_config('search_path', $10, true)")
    expect(statement.values[9]).toBe('storage,public,extensions')
  })

  it('uses placeholder ten when only statement timeout is present', () => {
    const statement = buildScopeStatement(baseScopeOptions, role, claims, headers, 4321)

    expect(statement.text).toContain("set_config('statement_timeout', $10, true)")
    expect(statement.text).not.toContain("set_config('search_path'")
    expect(statement.values[9]).toBe('4321ms')
  })
})
