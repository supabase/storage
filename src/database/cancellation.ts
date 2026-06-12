import type { PoolClient } from 'pg'
import PgConnection from 'pg/lib/connection'

export type CancellableClient = PoolClient & {
  processID?: number
  secretKey?: number
  host?: string | string[]
  port?: number
  connectionParameters?: {
    host?: string | string[]
    port?: number
  }
}

export type InFlightOperation = {
  client?: CancellableClient
  lockId?: string
  cancelled: boolean
}

export class CancellationRegistry {
  private readonly operations = new Map<string, InFlightOperation>()

  start(requestId: string | undefined, operation: InFlightOperation): void {
    if (requestId) {
      this.operations.set(requestId, operation)
    }
  }

  setClient(requestId: string | undefined, client: CancellableClient): void {
    if (!requestId) {
      return
    }

    const operation = this.operations.get(requestId)
    if (operation) {
      operation.client = client
    }
  }

  finish(requestId: string | undefined): void {
    if (requestId) {
      this.operations.delete(requestId)
    }
  }

  async cancel(requestId: string, lockId?: string): Promise<{ cancelled: boolean }> {
    const operation = this.operations.get(requestId)
    if (!operation) {
      return { cancelled: false }
    }

    if (lockId && operation.lockId && operation.lockId !== lockId) {
      return { cancelled: false }
    }

    operation.cancelled = true
    if (operation.client) {
      await cancelPgQuery(operation.client)
    }

    return { cancelled: true }
  }
}

async function cancelPgQuery(client: CancellableClient): Promise<void> {
  const processID = client.processID
  const secretKey = client.secretKey

  if (!processID || !secretKey) {
    return
  }

  const cancelConnection = new PgConnection()
  cancelConnection.unref()
  const target = getPgCancelConnectionTarget(client)

  return new Promise((resolve) => {
    let resolved = false

    const done = () => {
      if (resolved) {
        return
      }

      resolved = true
      clearTimeout(timeout)
      cancelConnection.end()
      resolve()
    }

    const timeout = setTimeout(done, 5_000)
    timeout.unref()

    cancelConnection.on('error', done)
    cancelConnection.on('end', done)
    cancelConnection.on('connect', () => {
      try {
        cancelConnection.cancel(processID, secretKey)
      } catch {
        done()
      }
    })

    if (target.type === 'socket') {
      cancelConnection.connect(target.path)
    } else {
      cancelConnection.connect(target.port, target.host)
    }
  })
}

function getPgCancelConnectionTarget(
  client: Pick<CancellableClient, 'host' | 'port' | 'connectionParameters'>
): { type: 'socket'; path: string } | { type: 'tcp'; host: string; port: number } {
  const rawHost = client.host || client.connectionParameters?.host || 'localhost'
  const host = Array.isArray(rawHost) ? rawHost[0] || 'localhost' : rawHost
  const port = client.port || client.connectionParameters?.port || 5432

  if (host.startsWith('/')) {
    return {
      type: 'socket',
      path: `${host}/.s.PGSQL.${port}`,
    }
  }

  return {
    type: 'tcp',
    host,
    port,
  }
}
