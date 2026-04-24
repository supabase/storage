import { vi } from 'vitest'

type MultitenantPgModule = typeof import('./multitenant-pg')

async function loadMultitenantPgModule(
  configOverrides: Record<string, unknown> = {}
): Promise<MultitenantPgModule> {
  vi.resetModules()

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    databaseApplicationName: 'storage-test',
    multitenantDatabaseUrl: 'postgres://user:password@db.example.test:5432/postgres',
    multitenantDatabasePoolUrl: undefined,
    multitenantMaxConnections: 3,
    ...configOverrides,
  } as Parameters<typeof configModule.mergeConfig>[0])

  return import('./multitenant-pg')
}

describe('multitenant pg pool', () => {
  let loadedModule: MultitenantPgModule | undefined

  afterEach(async () => {
    await loadedModule?.closeMultitenantPg()
    loadedModule = undefined
    vi.resetModules()
  })

  it('plumbs DATABASE_SSL_ROOT_CERT into the shared multitenant pool config', async () => {
    loadedModule = await loadMultitenantPgModule({
      databaseSSLRootCert: 'root-cert',
    })

    expect(loadedModule.getMultitenantPgPoolConfig()).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@db.example.test:5432/postgres',
        max: 3,
        ssl: {
          ca: 'root-cert',
        },
      })
    )
  })

  it('uses the pool URL for SSL settings and pool sizing when configured', async () => {
    loadedModule = await loadMultitenantPgModule({
      databaseSSLRootCert: 'root-cert',
      multitenantDatabasePoolUrl: 'postgres://user:password@1.2.3.4:6432/postgres',
    })

    expect(loadedModule.getMultitenantPgPoolConfig()).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@1.2.3.4:6432/postgres',
        max: 30,
        ssl: {
          ca: 'root-cert',
          rejectUnauthorized: false,
        },
      })
    )
  })

  it('reads the current config after runtime config changes', async () => {
    loadedModule = await loadMultitenantPgModule()

    expect(loadedModule.getMultitenantPgPoolConfig()).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@db.example.test:5432/postgres',
        max: 3,
      })
    )

    const configModule = await import('../../config')
    configModule.mergeConfig({
      multitenantDatabaseUrl: 'postgres://user:password@db2.example.test:5432/postgres',
      multitenantMaxConnections: 5,
    })

    expect(loadedModule.getMultitenantPgPoolConfig()).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@db2.example.test:5432/postgres',
        max: 5,
      })
    )
  })

  it('routes the exported pool proxy through the current config', async () => {
    loadedModule = await loadMultitenantPgModule()

    expect(getLoadedPoolOptions(loadedModule)).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@db.example.test:5432/postgres',
        max: 3,
      })
    )

    const configModule = await import('../../config')
    configModule.mergeConfig({
      multitenantDatabasePoolUrl: 'postgres://user:password@1.2.3.4:6432/postgres',
      multitenantMaxConnections: 4,
    })

    expect(getLoadedPoolOptions(loadedModule)).toEqual(
      expect.objectContaining({
        connectionString: 'postgres://user:password@1.2.3.4:6432/postgres',
        max: 40,
      })
    )
  })
})

function getLoadedPoolOptions(module: MultitenantPgModule): {
  connectionString?: string
  max?: number
} {
  return (
    module.multitenantPgPool as unknown as { options: { connectionString?: string; max?: number } }
  ).options
}
