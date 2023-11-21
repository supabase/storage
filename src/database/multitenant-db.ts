import Knex from 'knex'
import { getConfig } from '../config'

const { multitenantDatabaseUrl } = getConfig()

export const knex = Knex({
  client: 'pg',
  connection: multitenantDatabaseUrl,
  pool: {
    max: 5,
    min: 0,
    destroyTimeoutMillis: 2000,
  },
})
