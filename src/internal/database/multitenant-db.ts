import Knex from 'knex'
import { getConfig } from '../../config'

const {
  multitenantDatabaseUrl,
  multitenantDatabasePoolUrl,
  multitenantMaxConnections,
  databaseApplicationName,
} = getConfig()

const poolSize = multitenantDatabasePoolUrl
  ? multitenantMaxConnections * 10
  : multitenantMaxConnections

export const multitenantKnex = Knex({
  client: 'pg',
  connection: {
    connectionString: multitenantDatabasePoolUrl || multitenantDatabaseUrl,
    connectionTimeoutMillis: 5000,
    application_name: databaseApplicationName,
  },
  version: '12',
  pool: {
    min: 0,
    max: poolSize,
    createTimeoutMillis: 5000,
    acquireTimeoutMillis: 5000,
    idleTimeoutMillis: 5000,
    reapIntervalMillis: 1000,
    createRetryIntervalMillis: 100,
  },
})
