import type { Dirent } from 'node:fs'
import { readdir, stat } from 'node:fs/promises'
import { createServer, type Server as HttpServer, type IncomingMessage } from 'node:http'
import path from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3'
import { getAcceptanceConfig, requireConfigValue } from './config'

/**
 * Local-only checksum harness: poses as the server's Logflare sink to capture logs
 * compares [Lifecycle] log's bytesChanged against actual bytes in the backend
 */

export interface UsageLedgerEntry {
  bytesChanged: number
  event?: string
  objectPath?: string
  tenantId?: string
  timestamp?: number
}

export interface UsageLedgerSummary {
  baselineBytes: number
  count: number
  recentEntries: UsageLedgerEntry[]
  totalBytesChanged: number
  violations: string[]
}

const RECENT_ENTRIES_LIMIT = 20
const VIOLATIONS_LIMIT = 20

export function createUsageLedgerServer(): Promise<{ server: HttpServer; url: string }> {
  let baselineBytes = 0
  let totalBytesChanged = 0
  let count = 0
  const recentEntries: UsageLedgerEntry[] = []
  const violations: string[] = []

  function recordEntry(entry: UsageLedgerEntry) {
    totalBytesChanged += entry.bytesChanged
    count += 1
    recentEntries.push(entry)
    if (recentEntries.length > RECENT_ENTRIES_LIMIT) {
      recentEntries.shift()
    }
  }

  return new Promise((resolve, reject) => {
    const server = createServer((request, response) => {
      handleRequest(request, response).catch((error) => {
        response.writeHead(500, { 'content-type': 'application/json' })
        response.end(JSON.stringify({ message: String(error) }))
      })
    })

    async function handleRequest(request: IncomingMessage, response: NodeJS.WritableStream) {
      const url = new URL(request.url ?? '/', 'http://127.0.0.1')

      if (request.method === 'POST' && url.pathname === '/logs') {
        const body = await readJsonBody(request)
        const batch = Array.isArray((body as { batch?: unknown[] })?.batch)
          ? ((body as { batch: unknown[] }).batch as Record<string, unknown>[])
          : []

        for (const item of batch) {
          const metadata = item.metadata as Record<string, unknown> | undefined
          const bytesChanged = metadata?.bytesChanged

          if (typeof bytesChanged !== 'number') {
            continue
          }

          recordEntry({
            bytesChanged,
            event: typeof metadata?.event === 'string' ? metadata.event : undefined,
            objectPath: typeof metadata?.objectPath === 'string' ? metadata.objectPath : undefined,
            tenantId: typeof metadata?.tenantId === 'string' ? metadata.tenantId : undefined,
            timestamp: typeof item.timestamp === 'number' ? item.timestamp : undefined,
          })
        }

        writeJson(response, 200, {})
        return
      }

      if (request.method === 'POST' && url.pathname === '/__ledger/baseline') {
        const body = (await readJsonBody(request)) as { baselineBytes?: number }
        baselineBytes = body.baselineBytes ?? 0
        writeJson(response, 200, {})
        return
      }

      if (request.method === 'POST' && url.pathname === '/__ledger/violation') {
        const body = (await readJsonBody(request)) as { message?: string }
        if (body.message) {
          violations.push(body.message)
          if (violations.length > VIOLATIONS_LIMIT) {
            violations.shift()
          }
        }
        writeJson(response, 200, {})
        return
      }

      if (request.method === 'GET' && url.pathname === '/__ledger/summary') {
        const summary: UsageLedgerSummary = {
          baselineBytes,
          count,
          recentEntries: [...recentEntries],
          totalBytesChanged,
          violations: [...violations],
        }
        writeJson(response, 200, summary)
        return
      }

      writeJson(response, 404, { message: 'not found' })
    }

    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject)
      const address = server.address()
      if (!address || typeof address === 'string') {
        reject(new Error('Usage ledger stub did not bind to a TCP port'))
        return
      }

      resolve({ server, url: `http://127.0.0.1:${address.port}` })
    })
  })
}

function readJsonBody(request: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    request.on('data', (chunk: Buffer) => chunks.push(chunk))
    request.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8')
      if (!raw) {
        resolve(undefined)
        return
      }
      try {
        resolve(JSON.parse(raw))
      } catch (error) {
        reject(error)
      }
    })
    request.on('error', reject)
  })
}

function writeJson(response: NodeJS.WritableStream, status: number, body: unknown) {
  const writable = response as unknown as {
    writeHead: (status: number, headers: Record<string, string>) => void
    end: (body: string) => void
  }
  writable.writeHead(status, { 'content-type': 'application/json' })
  writable.end(JSON.stringify(body))
}

export async function fetchUsageLedgerSummary(ledgerUrl: string): Promise<UsageLedgerSummary> {
  const response = await fetch(`${ledgerUrl}/__ledger/summary`)
  return (await response.json()) as UsageLedgerSummary
}

export async function postUsageLedgerBaseline(
  ledgerUrl: string,
  baselineBytes: number
): Promise<void> {
  await fetch(`${ledgerUrl}/__ledger/baseline`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ baselineBytes }),
  })
}

const TUS_S3_STORE_INFO_SUFFIX = '.info'
const TUS_FILE_STORE_INFO_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$/i

export async function getBackendTotalBytes(options: { prefix?: string } = {}): Promise<number> {
  const backend = getAcceptanceConfig().storageBackend ?? 's3'

  if (backend === 'file') {
    return getFileBackendTotalBytes(options.prefix)
  }

  return getS3BackendTotalBytes(options.prefix)
}

async function getS3BackendTotalBytes(prefix?: string): Promise<number> {
  const config = getAcceptanceConfig()
  const bucket = requireConfigValue(config.storageS3Bucket, 'STORAGE_S3_BUCKET')
  const endpoint = requireConfigValue(config.storageS3Endpoint, 'STORAGE_S3_ENDPOINT')

  // No explicit `credentials` - AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (forwarded from the
  // server's own env by run-managed-local.ts) are picked up by the SDK's default chain.
  const client = new S3Client({
    endpoint,
    forcePathStyle: config.storageS3ForcePathStyle,
    region: config.storageS3Region,
  })

  let total = 0
  let continuationToken: string | undefined

  try {
    do {
      const page = await client.send(
        new ListObjectsV2Command({
          Bucket: bucket,
          ContinuationToken: continuationToken,
          Prefix: prefix,
        })
      )

      for (const object of page.Contents ?? []) {
        if (object.Key?.endsWith(TUS_S3_STORE_INFO_SUFFIX)) {
          continue
        }
        total += object.Size ?? 0
      }

      continuationToken = page.NextContinuationToken
    } while (continuationToken)
  } finally {
    client.destroy()
  }

  return total
}

async function getFileBackendTotalBytes(prefix?: string): Promise<number> {
  const config = getAcceptanceConfig()
  const backendPath = requireConfigValue(config.storageFileBackendPath, 'STORAGE_FILE_BACKEND_PATH')
  const bucket = requireConfigValue(config.storageS3Bucket, 'STORAGE_S3_BUCKET')
  const root = prefix ? path.join(backendPath, bucket, prefix) : path.join(backendPath, bucket)

  return sumDirectorySize(root)
}

async function sumDirectorySize(dir: string): Promise<number> {
  let entries: Dirent[]
  try {
    entries = await readdir(dir, { withFileTypes: true })
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return 0
    }
    throw error
  }

  let total = 0

  for (const entry of entries) {
    const entryPath = path.join(dir, entry.name)

    if (entry.isDirectory()) {
      total += await sumDirectorySize(entryPath)
      continue
    }

    if (entry.isFile() && !TUS_FILE_STORE_INFO_PATTERN.test(entry.name)) {
      total += (await stat(entryPath)).size
    }
  }

  return total
}

function describeMismatch(
  label: string,
  expectedBackendDelta: number,
  summary: UsageLedgerSummary
): string {
  return [
    `Usage byte checksum mismatch at "${label}":`,
    `backend delta=${expectedBackendDelta}, logged bytesChanged sum=${summary.totalBytesChanged}`,
    `(${summary.count} [Lifecycle] log entries observed so far)`,
    `recent entries: ${JSON.stringify(summary.recentEntries)}`,
  ].join(' ')
}

async function reportLedgerViolation(ledgerUrl: string, message: string): Promise<void> {
  console.warn(message)
  await fetch(`${ledgerUrl}/__ledger/violation`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ message }),
  }).catch(() => undefined)
}

/**
 * These logs ship over an async pino worker-thread transport, so there's a short, expected
 * lag before a mismatch resolves itself - poll rather than compare once.
 *
 * Never throws: this runs from shared cleanup helpers, often inside a `finally`, and raising
 * here would replace a real test's own assertion error with a checksum error. A persistent
 * mismatch is recorded as a "violation" instead, surfaced by usage-checksum.test.ts at the end.
 */
export async function checkpointUsageBytes(label: string): Promise<void> {
  const { usageLedgerUrl: ledgerUrl, usagePrefix: prefix } = getAcceptanceConfig()
  if (!ledgerUrl) {
    return
  }

  const intervalMs = 150
  const timeoutMs = 5_000
  const deadline = Date.now() + timeoutMs

  let lastMismatchMessage: string | undefined

  while (Date.now() < deadline) {
    try {
      const summary = await fetchUsageLedgerSummary(ledgerUrl)
      const actualBytes = await getBackendTotalBytes({ prefix })
      const expectedBackendDelta = actualBytes - summary.baselineBytes

      if (expectedBackendDelta === summary.totalBytesChanged) {
        return
      }

      lastMismatchMessage = describeMismatch(label, expectedBackendDelta, summary)
    } catch (error) {
      lastMismatchMessage = `Usage checksum error at "${label}": ${String(error)}`
    }

    await delay(intervalMs)
  }

  if (lastMismatchMessage) {
    await reportLedgerViolation(ledgerUrl, lastMismatchMessage)
  }
}
