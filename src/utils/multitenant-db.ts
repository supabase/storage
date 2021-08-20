import { Pool } from 'pg'
import { getConfig } from './config'

const { multitenantDatabaseUrl } = getConfig()

export const pool = new Pool({
  connectionString: multitenantDatabaseUrl,
})
