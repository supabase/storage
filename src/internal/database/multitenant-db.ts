import Knex from 'knex'
import { getConfig } from '../../config'

const { multitenantDatabaseUrl } = getConfig()

export const multitenantKnex = Knex({
  client: 'pg',
  connection: multitenantDatabaseUrl,
  pool: {
    min: 0,
    max: 10,
  },
})
