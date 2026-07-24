import { removeGlobals, updateGlobals } from '@platformatic/globals'
import { afterEach, describe, expect, it, vi } from 'vitest'

type LoadClientOptions = {
  isMultitenant?: boolean
  hasWattMessaging?: boolean
  databaseWattApplicationEnabled?: boolean
  disableHostCheck?: boolean
}

afterEach(() => {
  vi.doUnmock('@internal/cluster')
  vi.doUnmock('@internal/errors')
  vi.doUnmock('../../config')
  vi.doUnmock('./pg-connection')
  vi.doUnmock('./tenant')
  vi.doUnmock('./watt/connection')
  vi.resetModules()
  removeGlobals(['messaging'])
})

describe('database connection client', () => {
  it('uses Database Watt when messaging is available', async () => {
    const { client, getTenantConfig, getWattPostgresConnection, wattConnection } = await loadClient(
      {
        hasWattMessaging: true,
      }
    )

    const connection = await client.getPostgresConnection(createConnectionOptions())

    expect(connection).toBe(wattConnection)
    expect(getTenantConfig).not.toHaveBeenCalled()
    expect(getWattPostgresConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        dbUrl: '',
        isExternalPool: false,
        maxConnections: 20,
        tenantId: 'tenant-a',
      })
    )
  })

  it('falls back to direct PostgreSQL when Watt messaging is unavailable', async () => {
    const { client, getTenantConfig, pgConnection, pgCreate } = await loadClient({
      hasWattMessaging: false,
    })

    const connection = await client.getPostgresConnection(createConnectionOptions())

    expect(connection).toBe(pgConnection)
    expect(getTenantConfig).toHaveBeenCalledWith('tenant-a')
    expect(pgCreate).toHaveBeenCalledWith(
      expect.objectContaining({
        clusterSize: 3,
        dbUrl: 'postgres://tenant-db',
        maxConnections: 7,
      })
    )
  })

  it('falls back to direct PostgreSQL when the Database Watt application is disabled', async () => {
    const { client, getTenantConfig, getWattPostgresConnection, pgConnection, pgCreate } =
      await loadClient({
        databaseWattApplicationEnabled: false,
        hasWattMessaging: true,
      })

    const connection = await client.getPostgresConnection(createConnectionOptions())

    expect(connection).toBe(pgConnection)
    expect(getWattPostgresConnection).not.toHaveBeenCalled()
    expect(getTenantConfig).toHaveBeenCalledWith('tenant-a')
    expect(pgCreate).toHaveBeenCalledWith(
      expect.objectContaining({
        clusterSize: 3,
        dbUrl: 'postgres://tenant-db',
        maxConnections: 7,
      })
    )
  })

  it('validates multitenant forwarded host before routing to Database Watt', async () => {
    const { client, getWattPostgresConnection } = await loadClient({
      hasWattMessaging: true,
      isMultitenant: true,
    })

    await expect(
      client.getPostgresConnection(
        createConnectionOptions({ host: 'evil.example.test', disableHostCheck: false })
      )
    ).rejects.toThrow('X-Forwarded-Host header does not match regular expression')

    expect(getWattPostgresConnection).not.toHaveBeenCalled()
  })
})

async function loadClient(options: LoadClientOptions = {}) {
  vi.resetModules()

  const getTenantConfig = vi.fn(async () => ({
    databasePoolUrl: undefined,
    databaseUrl: 'postgres://tenant-db',
    maxConnections: 7,
  }))
  const pgConnection = { kind: 'pg-connection' }
  const wattConnection = { kind: 'watt-connection' }
  const pgCreate = vi.fn(async () => pgConnection)
  const getWattPostgresConnection = vi.fn(async () => wattConnection)

  vi.doMock('@internal/cluster', () => ({ Cluster: { size: 3 } }))
  vi.doMock('@internal/errors', () => ({
    ERRORS: {
      InvalidTenantId: () => new Error('Invalid tenant id'),
      InvalidXForwardedHeader: (message: string) => new Error(message),
    },
  }))
  vi.doMock('../../config', () => ({
    getConfig: () => ({
      databaseMaxConnections: 20,
      databasePoolURL: undefined,
      databaseURL: 'postgres://default-db',
      databaseWattApplicationEnabled: options.databaseWattApplicationEnabled ?? true,
      isMultitenant: options.isMultitenant ?? true,
      requestXForwardedHostRegExp: '^tenant-[a-z]+\\.example\\.test$',
    }),
  }))
  vi.doMock('./pg-connection', () => ({
    PgTenantConnection: { create: pgCreate },
  }))
  vi.doMock('./tenant', () => ({ getTenantConfig }))
  vi.doMock('./watt/connection', () => ({
    getWattPostgresConnection,
  }))

  if (options.hasWattMessaging) {
    updateGlobals({
      messaging: {
        handle: vi.fn(),
        notify: vi.fn(),
        send: vi.fn(),
      },
    })
  } else {
    removeGlobals(['messaging'])
  }

  const client = await import('./client')
  return {
    client,
    getTenantConfig,
    getWattPostgresConnection,
    pgConnection,
    pgCreate,
    wattConnection,
  }
}

function createConnectionOptions(
  overrides: Partial<Parameters<typeof import('./client').getPostgresConnection>[0]> = {}
) {
  return {
    disableHostCheck: true,
    host: 'tenant-a.example.test',
    superUser: {
      jwt: 'service-jwt',
      payload: { role: 'service_role' },
    },
    tenantId: 'tenant-a',
    user: {
      jwt: 'user-jwt',
      payload: { role: 'authenticated' },
    },
    ...overrides,
  }
}
