export interface PubSubAdapter {
  start(): Promise<void>
  publish(channel: string, message: any): Promise<void>
  subscribe(channel: string, cb: (message: any) => void): Promise<void>
  unsubscribe(channel: string, cb: (message: any) => void): Promise<void>
  close(): Promise<void>
  on(event: 'error', listener: (error: Error) => void): this
}
