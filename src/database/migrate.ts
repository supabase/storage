import { Client, ClientConfig } from 'pg'
import { migrate } from 'postgres-migrations'
import { getConfig } from '../config'

const { multitenantDatabaseUrl, databaseSSLRootCert } = getConfig()

/**
 * Runs tenant migrations
 */
export async function runMigrations(): Promise<void> {
  console.log('running migrations')
  let ssl: ClientConfig['ssl'] | undefined = undefined

  if (databaseSSLRootCert) {
    ssl = { ca: databaseSSLRootCert }
  }
  await connectAndMigrate(process.env.DATABASE_URL, './migrations/tenant', ssl)
  console.log('finished migrations')
}

/**
 * Runs multi-tenant migrations
 */
export async function runMultitenantMigrations(): Promise<void> {
  console.log('running multitenant migrations')
  await connectAndMigrate(multitenantDatabaseUrl, './migrations/multitenant')
  console.log('finished multitenant migrations')
}

/**
 * Runs migrations on a specific tenant by providing its database DSN
 * @param databaseUrl
 */
export async function runMigrationsOnTenant(databaseUrl: string): Promise<void> {
  let ssl: ClientConfig['ssl'] | undefined = undefined

  if (databaseSSLRootCert) {
    ssl = { ca: databaseSSLRootCert }
  }
  await connectAndMigrate(databaseUrl, './migrations/tenant', ssl)
}

async function connectAndMigrate(
  databaseUrl: string | undefined,
  migrationsDirectory: string,
  ssl?: ClientConfig['ssl']
) {
  const dbConfig: ClientConfig = {
    connectionString: databaseUrl,
    connectionTimeoutMillis: 10_000,
    options: '-c search_path=storage,public',
    ssl,
  }

  const client = new Client(dbConfig)
  try {
    await client.connect()
    await migrate({ client }, migrationsDirectory)
  } finally {
    await client.end()
  }
}
