import Knex from 'knex'
import { getConfig } from '../../config'

const { multitenantDatabaseUrl, multitenantDatabasePoolUrl } = getConfig()

export const multitenantKnex = Knex({
  client: 'pg',
  connection: {
    connectionString: multitenantDatabasePoolUrl || multitenantDatabaseUrl,
    connectionTimeoutMillis: 5000,
  },
  version: '12',
  pool: {
    min: 0,
    max: 200,
    createTimeoutMillis: 5000,
    acquireTimeoutMillis: 5000,
    idleTimeoutMillis: 5000,
    reapIntervalMillis: 1000,
    createRetryIntervalMillis: 100,
  },
})
