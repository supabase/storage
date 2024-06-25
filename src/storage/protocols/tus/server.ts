import { ERRORS, Server } from '@tus/server'
import http from 'node:http'

export class TusServer extends Server {
  protected createContext(req: http.IncomingMessage) {
    // Initialize two AbortControllers:
    // 1. `requestAbortController` for instant request termination, particularly useful for stopping clients to upload when errors occur.
    // 2. `abortWithDelayController` to introduce a delay before aborting, allowing the server time to complete ongoing operations.
    // This is particularly useful when a future request may need to acquire a lock currently held by this request.
    const requestAbortController = new AbortController()
    const abortWithDelayController = new AbortController()

    const onDelayedAbort = (err: unknown) => {
      abortWithDelayController.signal.removeEventListener('abort', onDelayedAbort)
      setTimeout(() => {
        requestAbortController.abort(err)
      }, 200)
    }
    abortWithDelayController.signal.addEventListener('abort', onDelayedAbort)

    req.on('close', () => {
      abortWithDelayController.signal.removeEventListener('abort', onDelayedAbort)
    })

    return {
      signal: requestAbortController.signal,
      abort: () => {
        // abort the request immediately
        if (!requestAbortController.signal.aborted) {
          requestAbortController.abort(ERRORS.ABORTED)
        }
      },
      cancel: () => {
        // Initiates the delayed abort sequence unless it's already in progress.
        if (!abortWithDelayController.signal.aborted) {
          abortWithDelayController.abort(ERRORS.ABORTED)
        }
      },
    }
  }
}
