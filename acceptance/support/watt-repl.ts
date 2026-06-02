import { RuntimeApiClient } from '@platformatic/control'

type ReplWebSocket = {
  close(): void
  off(event: string, listener: (...args: any[]) => void): void
  on(event: string, listener: (...args: any[]) => void): void
  send(data: string): void
}

type RuntimeApiClientWithRepl = RuntimeApiClient & {
  getRuntimeApplicationRepl(pid: number, applicationId: string): ReplWebSocket
}

type ReplResult<T> =
  | { ok: true; result: T }
  | { ok: false; error: { code?: string; message: string; stack?: string } }

const resultPrefix = '__DATABASE_WATT_REPL_RESULT_START__'
const resultSuffix = '__DATABASE_WATT_REPL_RESULT_END__'

function waitForReplResult<T>(repl: ReplWebSocket, script: string): Promise<T> {
  return new Promise((resolve, reject) => {
    let output = ''
    let sent = false
    const timeout = setTimeout(() => {
      cleanup()
      reject(new Error(`Timed out waiting for Watt REPL result. Output:\n${output}`))
    }, 30_000)

    function cleanup() {
      clearTimeout(timeout)
      repl.off('open', onOpen)
      repl.off('message', onMessage)
      repl.off('error', onError)
      repl.off('close', onClose)
    }

    function onOpen() {
      sendWhenReady()
    }

    function onMessage(data: { toString(): string }) {
      output += data.toString()
      sendWhenReady()

      const resultIndex = output.indexOf(resultPrefix)

      if (resultIndex === -1) {
        return
      }

      const resultStart = resultIndex + resultPrefix.length
      const resultEnd = output.indexOf(resultSuffix, resultStart)

      if (resultEnd === -1) {
        return
      }

      const resultLine = output.slice(resultStart, resultEnd)
      let parsed: ReplResult<T>

      try {
        parsed = JSON.parse(resultLine)
      } catch (error) {
        cleanup()
        reject(new Error(`Failed to parse Watt REPL result: ${String(error)}\nOutput:\n${output}`))
        return
      }

      cleanup()
      if (parsed.ok) {
        resolve(parsed.result)
      } else {
        const replError = new Error(parsed.error.message)
        if (parsed.error.stack) {
          replError.stack = parsed.error.stack
        }
        ;(replError as Error & { code?: string }).code = parsed.error.code
        reject(replError)
      }
    }

    function sendWhenReady() {
      if (sent || !output.includes('storage> ')) {
        return
      }

      sent = true
      repl.send(`${script}\n`)
    }

    function onError(error: Error) {
      cleanup()
      reject(error)
    }

    function onClose() {
      cleanup()
      reject(new Error(`Watt REPL closed before returning a result. Output:\n${output}`))
    }

    repl.on('open', onOpen)
    repl.on('message', onMessage)
    repl.on('error', onError)
    repl.on('close', onClose)
  })
}

async function runStorageRepl<T>(script: string): Promise<T> {
  const client = new RuntimeApiClient() as RuntimeApiClientWithRepl
  const runtime = await client.getMatchingRuntime()
  const repl = client.getRuntimeApplicationRepl(runtime.pid, 'storage')

  try {
    const result = await waitForReplResult<T>(repl, script)
    return result
  } finally {
    repl.close()
    await client.close()
  }
}

export async function sendWattMessage<T = unknown>(application: string, message: string, payload: unknown): Promise<T> {
  return runStorageRepl<T>(
    `platformatic.messaging.send(${JSON.stringify(application)}, ${JSON.stringify(message)}, ${JSON.stringify(payload)})` +
      `.then(result => console.log(${JSON.stringify(resultPrefix)} + JSON.stringify({ ok: true, result }) + ${JSON.stringify(resultSuffix)}))` +
      `.catch(error => console.log(${JSON.stringify(resultPrefix)} + JSON.stringify({ ok: false, error: { code: error && typeof error === 'object' && 'code' in error ? error.code : undefined, message: error instanceof Error ? error.message : String(error), stack: error instanceof Error ? error.stack : undefined } }) + ${JSON.stringify(resultSuffix)}))`
  )
}
