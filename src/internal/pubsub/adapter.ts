export interface PubSubAdapter {
  start(): Promise<void>
  publish<T>(channel: string, message: T): Promise<void>
  subscribe<T>(channel: string, cb: (message: T) => void): Promise<void>
  unsubscribe<T>(channel: string, cb: (message: T) => void): Promise<void>
  close(): Promise<void>
  on(event: 'error', listener: (error: Error) => void): this
}
