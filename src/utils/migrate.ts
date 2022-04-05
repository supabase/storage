import { Client } from 'pg'
import { migrate } from 'postgres-migrations'
import { getConfig } from './config'

const { multitenantDatabaseUrl } = getConfig()

async function connectAndMigrate(databaseUrl: string | undefined, migrationsDirectory: string) {
  const dbConfig = {
    connectionString: databaseUrl,
    connectionTimeoutMillis: 10_000,
  }
  const client = new Client(dbConfig)
  try {
    await client.connect()
    await migrate({ client }, migrationsDirectory)
  } finally {
    await client.end()
  }
}

export async function runMigrations(): Promise<void> {
  console.log('running migrations')
  await connectAndMigrate(process.env.DATABASE_URL, './migrations/tenant')
  console.log('finished migrations')
}

export async function runMultitenantMigrations(): Promise<void> {
  console.log('running multitenant migrations')
  await connectAndMigrate(multitenantDatabaseUrl, './migrations/multitenant')
  console.log('finished multitenant migrations')
}

export async function runMigrationsOnTenant(databaseUrl: string): Promise<void> {
  await connectAndMigrate(databaseUrl, './migrations/tenant')
}
