import { readFileSync } from 'node:fs'

export type DatabaseConfig = {
  applicationName: string
  acquireTimeoutMs: number
  connectionTimeoutMs: number
  destinationAcquireQueueLimit: number
  destinationMaxConnections: number
  globalAcquireQueueLimit: number
  globalMaxConnections: number
  idlePoolTimeoutMs: number
  lockIdleTimeoutMs: number
  lockMaxLifetimeMs: number
  masterIsExternalPool: boolean
  masterConnectionString?: string
  masterMaxConnections: number
  maxActivePools: number
  poolIsExternal: boolean
  poolConnectionString?: string
  rootCert?: string
  serverStatementTimeoutMs: number
  shutdownTimeoutMs: number
}

export function readConfig(env: NodeJS.ProcessEnv = process.env): DatabaseConfig {
  return {
    applicationName: env.DATABASE_APPLICATION_NAME || `Supabase Storage Database Watt`,
    acquireTimeoutMs: readPositiveInteger(env.DATABASE_WATT_ACQUIRE_TIMEOUT, 3_000),
    connectionTimeoutMs: readPositiveInteger(env.DATABASE_CONNECTION_TIMEOUT, 3_000),
    destinationAcquireQueueLimit: readPositiveInteger(
      env.DATABASE_WATT_DESTINATION_ACQUIRE_QUEUE_LIMIT,
      100
    ),
    destinationMaxConnections: readPositiveInteger(
      env.DATABASE_WATT_DESTINATION_MAX_CONNECTIONS || env.DATABASE_MAX_CONNECTIONS,
      20
    ),
    globalAcquireQueueLimit: readPositiveInteger(env.DATABASE_WATT_GLOBAL_ACQUIRE_QUEUE_LIMIT, 500),
    globalMaxConnections: readPositiveInteger(
      env.DATABASE_WATT_GLOBAL_MAX_CONNECTIONS || env.DATABASE_MAX_CONNECTIONS,
      20
    ),
    idlePoolTimeoutMs: readPositiveInteger(
      env.DATABASE_WATT_POOL_IDLE_TIMEOUT || env.DATABASE_FREE_POOL_AFTER_INACTIVITY,
      60_000
    ),
    lockIdleTimeoutMs: readPositiveInteger(env.DATABASE_WATT_LOCK_IDLE_TIMEOUT, 30_000),
    lockMaxLifetimeMs: readPositiveInteger(env.DATABASE_WATT_LOCK_MAX_LIFETIME, 120_000),
    masterConnectionString:
      isTruthy(env.MULTI_TENANT) || isTruthy(env.IS_MULTITENANT)
        ? env.DATABASE_MULTITENANT_POOL_URL || env.DATABASE_MULTITENANT_URL
        : undefined,
    masterIsExternalPool: Boolean(env.DATABASE_MULTITENANT_POOL_URL),
    masterMaxConnections: readPositiveInteger(
      env.DATABASE_MULTITENANT_MAX_CONNECTIONS || env.DATABASE_WATT_MASTER_MAX_CONNECTIONS,
      10
    ),
    maxActivePools: readPositiveInteger(env.DATABASE_WATT_MAX_ACTIVE_POOLS, 1_000),
    poolIsExternal: Boolean(env.DATABASE_POOL_URL),
    poolConnectionString: env.DATABASE_POOL_URL || env.DATABASE_URL,
    rootCert: readRootCert(env.DATABASE_SSL_ROOT_CERT),
    serverStatementTimeoutMs: readInteger(env.DATABASE_STATEMENT_TIMEOUT, 30_000),
    shutdownTimeoutMs: readPositiveInteger(env.DATABASE_WATT_SHUTDOWN_TIMEOUT, 10_000),
  }
}

function readInteger(value: string | undefined, fallback: number): number {
  if (value === undefined || value === '') {
    return fallback
  }

  const numberValue = Number.parseInt(value, 10)
  return Number.isFinite(numberValue) ? numberValue : fallback
}

function readPositiveInteger(value: string | undefined, fallback: number): number {
  return Math.max(readInteger(value, fallback), 0)
}

function readRootCert(value: string | undefined): string | undefined {
  if (!value) {
    return undefined
  }

  if (value.includes('-----BEGIN CERTIFICATE-----')) {
    return value
  }

  return readFileSync(value, 'utf8')
}

function isTruthy(value: string | undefined): boolean {
  return value === '1' || value === 'true' || value === 'yes'
}
