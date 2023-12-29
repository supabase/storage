export interface PubSubAdapter {
  connect(): Promise<void>
  publish(channel: string, message: any): Promise<void>
  subscribe(channel: string, cb: (message: any) => void): Promise<void>
  unsubscribe(channel: string, cb: (message: any) => void): Promise<void>
  close(): Promise<void>
}
