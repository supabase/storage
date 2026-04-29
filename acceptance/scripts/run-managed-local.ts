import { type ChildProcess, spawn } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import dotenv from 'dotenv'

// Snapshot before loading .env.acceptance so server config only comes from .env.test/.env.
const inheritedEnv = { ...process.env }
loadAcceptanceEnvFile()

const args = process.argv.slice(2)
const profile = readArg('profile') ?? acceptanceEnv('ACCEPTANCE_PROFILE') ?? 'smoke'
const serverEnv = loadServerEnvFiles(inheritedEnv)
const serverPort = serverEnv.SERVER_PORT || serverEnv.PORT || '5000'
const baseUrl = acceptanceEnv('ACCEPTANCE_BASE_URL') ?? `http://127.0.0.1:${serverPort}`
const acceptanceRunEnv: NodeJS.ProcessEnv = {
  ...process.env,
  ACCEPTANCE_BASE_URL: baseUrl,
  ACCEPTANCE_PROFILE: profile,
  ACCEPTANCE_S3_ENDPOINT: acceptanceEnv('ACCEPTANCE_S3_ENDPOINT') ?? `${baseUrl}/s3`,
  ACCEPTANCE_TUS_ENDPOINT:
    acceptanceEnv('ACCEPTANCE_TUS_ENDPOINT') ?? `${baseUrl}/upload/resumable`,
}

let server: ChildProcess | undefined
let provisionedS3Credential: ProvisionedS3Credential | undefined

main().catch((error) => {
  console.error(error)
  process.exit(1)
})

async function main() {
  try {
    if (process.env.ACCEPTANCE_SKIP_INFRA !== 'true') {
      await run('npm', ['run', resolveInfraRestartScript()], serverEnv)
      await run('npm', ['run', 'test:dummy-data'], serverEnv)
    }

    server = spawn(localBin('tsx'), ['src/start/server.ts'], {
      detached: process.platform !== 'win32',
      env: serverEnv,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    prefixOutput(server.stdout, '[storage] ')
    prefixOutput(server.stderr, '[storage] ')

    await waitForStatus(`${baseUrl}/status`, 60_000)

    if (isTruthy(serverEnv.MULTI_TENANT) || isTruthy(serverEnv.IS_MULTITENANT)) {
      provisionedS3Credential = await provisionLocalMultitenantTenant(serverEnv)
      acceptanceRunEnv.ACCEPTANCE_TENANT_ID = provisionedS3Credential.tenantId
      acceptanceRunEnv.ACCEPTANCE_S3_ACCESS_KEY_ID = provisionedS3Credential.accessKey
      acceptanceRunEnv.ACCEPTANCE_S3_SECRET_ACCESS_KEY = provisionedS3Credential.secretKey
    }

    await run('npm', ['run', 'acceptance:run', '--', ...args], acceptanceRunEnv)
  } finally {
    if (provisionedS3Credential) {
      await deleteProvisionedS3Credential(provisionedS3Credential).catch((error) => {
        process.stderr.write(
          `[acceptance] failed to delete local S3 credential: ${String(error)}\n`
        )
      })
    }

    if (server) {
      await stopServer(server)
    }
  }
}

interface ProvisionedS3Credential {
  accessKey: string
  adminApiKey: string
  adminUrl: string
  id: string
  secretKey: string
  tenantId: string
}

interface S3CredentialResponse {
  access_key?: string
  id?: string
  secret_key?: string
}

async function provisionLocalMultitenantTenant(
  env: NodeJS.ProcessEnv
): Promise<ProvisionedS3Credential> {
  const adminPort = env.SERVER_ADMIN_PORT || '5001'
  const adminUrl = `http://127.0.0.1:${adminPort}`
  const adminApiKey = firstCsvValue(requiredEnv(env, 'SERVER_ADMIN_API_KEYS', 'ADMIN_API_KEYS'))
  const tenantId = requiredEnv(env, 'TENANT_ID')

  await waitForStatus(`${adminUrl}/status`, 60_000)

  await requestAdmin(adminUrl, adminApiKey, 'PUT', `/tenants/${encodeURIComponent(tenantId)}`, {
    anonKey: requiredEnv(env, 'ANON_KEY'),
    databasePoolUrl: env.DATABASE_POOL_URL || undefined,
    databaseUrl: requiredEnv(env, 'DATABASE_URL'),
    features: {
      icebergCatalog: {
        enabled: isTruthy(env.ICEBERG_ENABLED),
        maxCatalogs: envNumber(env.ICEBERG_MAX_CATALOGS, 2),
        maxNamespaces: envNumber(env.ICEBERG_MAX_NAMESPACES, 25),
        maxTables: envNumber(env.ICEBERG_MAX_TABLES, 10),
      },
      imageTransformation: {
        enabled: isTruthy(env.IMAGE_TRANSFORMATION_ENABLED),
        maxResolution: envNumber(env.IMAGE_TRANSFORMATION_LIMIT_MAX_SIZE, 2000),
      },
      purgeCache: {
        enabled: false,
      },
      s3Protocol: {
        enabled: true,
      },
      vectorBuckets: {
        enabled: isTruthy(env.VECTOR_ENABLED),
        maxBuckets: envNumber(env.VECTOR_MAX_BUCKETS, 10),
        maxIndexes: envNumber(env.VECTOR_MAX_INDEXES, 20),
      },
    },
    fileSizeLimit: envNumber(env.UPLOAD_FILE_SIZE_LIMIT, 524288000),
    jwtSecret: requiredEnv(env, 'AUTH_JWT_SECRET', 'PGRST_JWT_SECRET'),
    serviceKey: requiredEnv(env, 'SERVICE_KEY'),
  })

  const credential = await requestAdmin<S3CredentialResponse>(
    adminUrl,
    adminApiKey,
    'POST',
    `/s3/${encodeURIComponent(tenantId)}/credentials`,
    {
      claims: {
        role: env.DB_SERVICE_ROLE || 'service_role',
        sub: 'local-acceptance',
      },
      description: `local-acceptance-${Date.now()}`,
    },
    201
  )

  if (!credential?.id || !credential.access_key || !credential.secret_key) {
    throw new Error('Local multitenant S3 credential response was incomplete')
  }

  return {
    accessKey: credential.access_key,
    adminApiKey,
    adminUrl,
    id: credential.id,
    secretKey: credential.secret_key,
    tenantId,
  }
}

async function deleteProvisionedS3Credential(credential: ProvisionedS3Credential) {
  await requestAdmin(
    credential.adminUrl,
    credential.adminApiKey,
    'DELETE',
    `/s3/${encodeURIComponent(credential.tenantId)}/credentials`,
    {
      id: credential.id,
    },
    [200, 204]
  )
}

async function requestAdmin<T = unknown>(
  adminUrl: string,
  adminApiKey: string,
  method: string,
  route: string,
  body?: Record<string, unknown>,
  expectedStatus: number | number[] = 204
): Promise<T | undefined> {
  const response = await fetch(new URL(route.replace(/^\/+/, ''), `${adminUrl}/`), {
    body: body ? JSON.stringify(body) : undefined,
    headers: {
      apikey: adminApiKey,
      ...(body ? { 'content-type': 'application/json' } : undefined),
    },
    method,
  })
  const text = await response.text()

  if (!statusMatches(response.status, expectedStatus)) {
    throw new Error(
      [
        `Unexpected admin status for ${method} ${route}`,
        `expected: ${Array.isArray(expectedStatus) ? expectedStatus.join(', ') : expectedStatus}`,
        `received: ${response.status}`,
        `body: ${text}`,
      ].join('\n')
    )
  }

  return parseJson<T>(text)
}

function resolveInfraRestartScript() {
  const script = acceptanceEnv('ACCEPTANCE_INFRA_RESTART_SCRIPT') ?? 'infra:restart:ci'
  const allowed = new Set(['infra:restart:ci', 'infra:restart:ci:oriole'])

  if (!allowed.has(script)) {
    throw new Error(`Unsupported ACCEPTANCE_INFRA_RESTART_SCRIPT: ${script}`)
  }

  return script
}

function requiredEnv(env: NodeJS.ProcessEnv, name: string, fallbackName?: string): string {
  const value = env[name] || (fallbackName ? env[fallbackName] : undefined)

  if (!value) {
    throw new Error(
      `Missing required local acceptance environment variable: ${
        fallbackName ? `${name} or ${fallbackName}` : name
      }`
    )
  }

  return value
}

function envNumber(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback
  }

  const number = Number(value)
  return Number.isFinite(number) ? number : fallback
}

function firstCsvValue(value: string): string {
  return value.split(',')[0]?.trim() || value
}

function isTruthy(value: string | undefined): boolean {
  return value === '1' || value === 'true' || value === 'yes'
}

function statusMatches(status: number, expected: number | number[]) {
  return Array.isArray(expected) ? expected.includes(status) : status === expected
}

function parseJson<T>(text: string): T | undefined {
  if (!text) {
    return undefined
  }

  return JSON.parse(text) as T
}

function run(cmd: string, runArgs: string[], runEnv: NodeJS.ProcessEnv): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(command(cmd), runArgs, {
      env: runEnv,
      stdio: 'inherit',
    })

    child.on('exit', (code, signal) => {
      if (code === 0) {
        resolve()
      } else if (signal) {
        reject(new Error(`${cmd} ${runArgs.join(' ')} terminated with ${signal}`))
      } else {
        reject(new Error(`${cmd} ${runArgs.join(' ')} exited with ${code}`))
      }
    })
    child.on('error', reject)
  })
}

async function waitForStatus(url: string, timeoutMs: number) {
  const started = Date.now()
  let lastError: unknown

  while (Date.now() - started < timeoutMs) {
    let response: Response | undefined

    try {
      response = await fetch(url)
      if (response.status === 200) {
        return
      }
    } catch (error) {
      lastError = error
    } finally {
      await response?.body?.cancel()
    }

    await new Promise((resolve) => setTimeout(resolve, 500))
  }

  throw new Error(`Timed out waiting for ${url}: ${String(lastError)}`)
}

async function stopServer(child: ChildProcess) {
  if (hasExited(child)) {
    return
  }

  if (process.platform === 'win32') {
    const exitedAfterKill = waitForExit(child, 5_000)
    child.kill()

    if (!(await exitedAfterKill)) {
      process.stderr.write('[storage] server did not exit after kill\n')
    }
    return
  }

  const exitedAfterTerminate = waitForExit(child, 5_000)
  killProcessTree(child, 'SIGTERM')

  if (await exitedAfterTerminate) {
    return
  }

  process.stderr.write('[storage] server did not exit after SIGTERM; sending SIGKILL\n')

  const exitedAfterKill = waitForExit(child, 2_000)
  killProcessTree(child, 'SIGKILL')

  if (!(await exitedAfterKill)) {
    process.stderr.write('[storage] server did not exit after SIGKILL\n')
  }
}

function waitForExit(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (hasExited(child)) {
    return Promise.resolve(true)
  }

  return new Promise((resolve) => {
    let timer: NodeJS.Timeout

    function cleanup() {
      clearTimeout(timer)
      child.off('exit', onExit)
    }

    function onExit() {
      cleanup()
      resolve(true)
    }

    child.once('exit', onExit)
    timer = setTimeout(() => {
      cleanup()
      resolve(hasExited(child))
    }, timeoutMs)
  })
}

function hasExited(child: ChildProcess): boolean {
  return child.exitCode !== null || child.signalCode !== null
}

function killProcessTree(child: ChildProcess, signal: NodeJS.Signals) {
  if (process.platform === 'win32' || !child.pid) {
    child.kill(signal)
    return
  }

  try {
    process.kill(-child.pid, signal)
  } catch (error) {
    if (!isNoSuchProcessError(error)) {
      throw error
    }
  }
}

function isNoSuchProcessError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === 'ESRCH'
}

function readArg(name: string) {
  const flag = `--${name}`
  const equalsPrefix = `${flag}=`

  for (let index = 0; index < args.length; index++) {
    if (args[index] === flag) {
      return args[index + 1]
    }
    if (args[index].startsWith(equalsPrefix)) {
      return args[index].slice(equalsPrefix.length)
    }
  }

  return undefined
}

function prefixOutput(stream: NodeJS.ReadableStream | null, prefix: string) {
  stream?.setEncoding('utf8')
  stream?.on('data', (chunk: string) => {
    for (const line of chunk.split(/\r?\n/)) {
      if (line) {
        process.stderr.write(`${prefix}${line}\n`)
      }
    }
  })
}

function command(cmd: string) {
  return process.platform === 'win32' ? `${cmd}.cmd` : cmd
}

function localBin(cmd: string) {
  return path.resolve('node_modules', '.bin', command(cmd))
}

function acceptanceEnv(name: string): string | undefined {
  const value = process.env[name]
  return value === undefined || value === '' ? undefined : value
}

function loadAcceptanceEnvFile() {
  const acceptanceEnvPath = process.env.ACCEPTANCE_ENV_FILE ?? '.env.acceptance'

  dotenv.config({ path: path.resolve(acceptanceEnvPath), override: false })
}

function loadServerEnvFiles(baseEnv: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const env = { ...baseEnv }

  loadEnvFileInto(env, '.env.test')
  loadEnvFileInto(env, '.env')

  return env
}

function loadEnvFileInto(env: NodeJS.ProcessEnv, envPath: string) {
  const resolvedPath = path.resolve(envPath)
  if (!fs.existsSync(resolvedPath)) {
    return
  }

  const parsed = dotenv.parse(fs.readFileSync(resolvedPath))
  for (const [name, value] of Object.entries(parsed)) {
    if (env[name] === undefined) {
      env[name] = value
    }
  }
}
