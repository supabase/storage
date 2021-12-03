import { Client } from 'pg'
import { migrate } from 'postgres-migrations'
import { getConfig } from './config'

const { multitenantDatabaseUrl } = getConfig()

async function connectAndMigrate(
  databaseUrl: string | undefined,
  migrationsDirectory: string,
  logOnError = false
) {
  const dbConfig = {
    connectionString: databaseUrl,
    connectionTimeoutMillis: 1000,
  }
  const client = new Client(dbConfig)
  try {
    await client.connect()
    await client.query('SET pg_stat_statements.track TO none')
    await migrate({ client }, migrationsDirectory)
    await client.query('SET pg_stat_statements.track TO DEFAULT')
  } catch (error) {
    if (logOnError) {
      console.error('Migration error:', error.message)
    } else {
      throw error
    }
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
  await connectAndMigrate(databaseUrl, './migrations/tenant', true)
}
