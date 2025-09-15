import { BasePayload, Event } from '@internal/queue'
import { randomUUID } from 'crypto'
import { Job } from 'pg-boss'

const callbacks = new Map<string, (params: any) => Promise<any>>()

export function eventually<T, P>(operation: string, params: P, fn: (params: P) => Promise<T>) {
  if (callbacks.has(operation)) {
    throw new Error(`Operation ${operation} is already registered`)
  }
  callbacks.set(operation, fn)

  return async function (this: { tenantId: string }) {
    try {
      return await fn(params)
    } catch (error) {
      const opId = randomUUID()
      const opName = `${operation}-${opId}`

      const job = {
        opId,
        operation: opName,
        params,
        $version: 'v1',
        tenant: { ref: this.tenantId, host: '' },
      }

      AsyncInvoker.send(job)
    }
  }
}

interface AsyncInvokerPayload<P extends { [key: string]: any } = { [key: string]: any }>
  extends BasePayload {
  opId: string
  operation: string
  params: P
}

class AsyncInvoker extends Event<AsyncInvokerPayload> {
  protected static invokers = new Map<string, (params: any) => Promise<any>>()
  static queueName = 'async-invoker'

  /**
   * Registers a function to handle a specific operation and immediately executes it.
   *
   * @param job
   * @param handler - Function that will handle this operation when invoked
   * @returns Promise with the result of sending the operation to the queue
   */
  static async registerAndExecute<TParams extends { [key: string]: any }, TResult>(
    job: AsyncInvokerPayload<TParams>,
    handler: (params: TParams) => Promise<TResult>
  ): Promise<string | void | null> {
    // Register the handler function for this operation
    this.registerHandler(job.operation, handler)

    // Execute the operation by sending the params
    return await AsyncInvoker.send(job)
  }

  /**
   * Registers a handler function for a specific operation.
   *
   * @param operation - Unique identifier for the operation
   * @param handler - Function that will handle this operation when invoked
   */
  private static registerHandler<TParams, TResult>(
    operation: string,
    handler: (params: TParams) => Promise<TResult>
  ): void {
    this.invokers.set(operation, handler)
  }
  static async handle<P>(job: Job<P>) {}
}
