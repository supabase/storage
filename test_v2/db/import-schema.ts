/* eslint-disable no-console */
import { promises as fs } from 'node:fs'
import path from 'node:path'
import { config as loadEnv } from 'dotenv'
import { Client } from 'pg'

// Load .env.test first (test overrides) then .env (defaults like DATABASE_URL).
// dotenv won't overwrite existing entries, so the precedence is preserved.
loadEnv({ path: path.resolve(__dirname, '../../.env.test') })
loadEnv({ path: path.resolve(__dirname, '../../.env') })

// Applies the auth schema required by storage RLS. Unlike the legacy
// import-dummy-data script this imports *schema only* — no fixture rows.
// Tests create users / buckets / objects on the fly via test_v2/helpers/factories.
const files = ['01-auth-schema.sql']

async function main() {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is not set — test_v2/db/import-schema.ts aborting.')
  }

  const client = new Client({ connectionString: process.env.DATABASE_URL })
  await client.connect()
  try {
    for (const name of files) {
      const sql = await fs.readFile(path.resolve(__dirname, name), 'utf-8')
      await client.query(sql)
      console.log(`[test_v2] applied ${name}`)
    }
  } finally {
    await client.end()
  }
}

void main().catch((err) => {
  console.error('[test_v2] import-schema failed:', err)
  process.exit(1)
})
