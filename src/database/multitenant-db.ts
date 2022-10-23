import Knex from 'knex'
import { getConfig } from '../config'

const { multitenantDatabaseUrl } = getConfig()

export const knex = Knex({
  client: 'pg',
  connection: multitenantDatabaseUrl,
})
