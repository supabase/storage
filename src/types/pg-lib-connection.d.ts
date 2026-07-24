declare module 'pg/lib/connection' {
  import { EventEmitter } from 'node:events'
  export default class PgConnection extends EventEmitter {
    end(): void
    unref(): void
    cancel(processID: number, secretKey: number): void
    connect(port: number, host: string): void
    connect(path: string): void
  }
}
