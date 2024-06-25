import { PostgresPubSub } from '../pubsub'
import { getConfig } from '../../config'
import { logger } from '../monitoring'

const { isMultitenant, databaseURL, multitenantDatabaseUrl } = getConfig()

const connectionString = isMultitenant ? (multitenantDatabaseUrl as string) : databaseURL
export const PubSub = new PostgresPubSub(connectionString)

PubSub.on('error', (err) => {
  logger.error('PubSub error', {
    type: 'pubsub',
    error: err,
  })
})
