import { PostgresPubSub } from '../pubsub'
import { getConfig } from '../config'

const { isMultitenant, databaseURL, multitenantDatabaseUrl } = getConfig()

const connectionString = isMultitenant ? (multitenantDatabaseUrl as string) : databaseURL
export const PubSub = new PostgresPubSub(connectionString)
