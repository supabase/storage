export interface Persistence {
  insertBatch<T>(table: string, records: T[]): Promise<void>
  beginTransaction(): Promise<void>
  commitTransaction(): Promise<void>
  rollbackTransaction(): Promise<void>
  rawQuery(query: string, bindings?: any[]): Promise<any>
}
