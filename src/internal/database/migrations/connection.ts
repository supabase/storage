import { ERRORS } from '@internal/errors'
import { Client, type ClientConfig } from 'pg'
import { getConfig } from '../../../config'
import { logger, logSchema } from '../../monitoring'
import { searchPath } from '../pool'
import { getSslSettings } from '../postgres/ssl'

const { databaseSSLRootCert, databaseConnectionTimeout, databaseStatementTimeout } = getConfig()

/** Connect using the shared migration connection settings. */
export async function connectMigrationDatabase(options: {
  connectionString?: string | undefined
  ssl?: ClientConfig['ssl']
  tenantId?: string
  connectionTimeoutMillis?: number
  query_timeout?: number
}) {
  const { ssl, tenantId, connectionString } = options

  const dbConfig: ClientConfig = {
    connectionString,
    connectionTimeoutMillis: options.connectionTimeoutMillis ?? 60_000,
    query_timeout: options.query_timeout,
    // Migrations run trusted dynamic PL/pgSQL that a multigres gateway
    // rejects by default; only this connection opts into multigres
    // per-connection bypass, instead of the whole gateway being unsafe.
    // No-op against a plain postgres backend (accepted as a placeholder GUC).
    // direct_connection is also accepted, but multigres.unsafe_connection is
    // more explicit and less likely to be confused with a real connection mode.
    options: `-c search_path=${searchPath} -c multigres.unsafe_connection=on${
      options.query_timeout ? ` -c statement_timeout=${options.query_timeout}` : ''
    }`,
    ssl,
  }

  const client = new Client(dbConfig)
  client.on('error', (err) => {
    logSchema.error(logger, 'Error on database connection', {
      type: 'error',
      error: err,
      project: tenantId,
    })
  })
  await client.connect()
  return client
}

/** Read schema capabilities without applying migrations or certifying control state. */
export async function readTenantMigrationVersion({
  databaseUrl,
  tenantId,
}: {
  databaseUrl: string
  tenantId?: string
}): Promise<string> {
  const client = await connectMigrationDatabase({
    connectionString: databaseUrl,
    tenantId,
    ssl: getSslSettings({ connectionString: databaseUrl, databaseSSLRootCert }),
    connectionTimeoutMillis: databaseConnectionTimeout,
    query_timeout: databaseStatementTimeout,
  })
  try {
    const result = await client.query<{ name: string }>(
      'SELECT name FROM storage.migrations ORDER BY id DESC LIMIT 1'
    )
    if (!result.rows[0]?.name) {
      throw ERRORS.InternalError(undefined, 'Tenant migration ledger is empty')
    }
    return result.rows[0].name
  } finally {
    await client.end()
  }
}
