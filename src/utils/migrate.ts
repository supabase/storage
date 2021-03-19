import { Client } from 'pg'
import { createDb, migrate } from 'postgres-migrations'

export async function runMigrations(): Promise<void> {
  console.log('running migrations')

  const dbConfig = {
    connectionString: process.env.DATABASE_URL,
  }
  const client = new Client(dbConfig)
  await client.connect()
  try {
    await createDb('postgres', { client })
    await migrate({ client }, './migrations')
  } finally {
    await client.end()
  }

  console.log('finished migrations')
}
