import pg, { type Connection, Query, type QueryResult, Result } from 'pg'

export type BatchQueryStatement = { text: string; values?: unknown[] }

// The parts of pg's connection a query writes to (its typings lag behind pg's methods).
type BatchQueryConnection = {
  stream: { cork(): void; uncork(): void }
  parse(query: { text: string }): void
  bind(config: { values: unknown[] }): void
  describe(config: { type: 'P'; name: string }): void
  execute(config: { portal: string; rows: number }): void
  flush(): void
  sync(): void
  sendCopyFail(message: string): void
}

// pg's Result, with the methods its Query uses to fill it in.
type ResultBuilder = Result & {
  addFields(fields: unknown[]): void
  parseRow(fields: unknown[]): unknown
  addRow(row: unknown): void
  addCommandComplete(message: unknown): void
}

type ResultTypes = ConstructorParameters<typeof Result>[1]

const noValues: unknown[] = []

// pg's own parameter mapping (Buffers, Dates, arrays, JSON), as its Query applies it.
function prepareValue(value: unknown): unknown {
  return (pg as unknown as { utils: { prepareValue(value: unknown): unknown } }).utils.prepareValue(
    value
  )
}

/**
 * BEGIN (optional), statements and COMMIT or ROLLBACK (optional), written to PostgreSQL at
 * once and ended by a single Sync, as one pg query: one round trip.
 *
 * After an error PostgreSQL skips every message up to the Sync, so nothing behind a failed
 * statement runs, a failed BEGIN included. Separate pg queries cannot do this: each ends with
 * its own Sync, and statements pipelined behind a failed BEGIN would each run in autocommit.
 *
 * BEGIN is followed by a Flush, so its reply comes back as soon as it has run: if the query
 * fails before that reply (`beganTransaction` is false), BEGIN did not run and nothing behind it
 * did either.
 *
 * It is a pg Query so that pipelining clients accept it: pg refuses other custom queries there,
 * because they can keep a portal open across round trips. This one ends with one Sync and gets
 * one ReadyForQuery, like any Query, and replaces every handler. It settles through `callback`,
 * as Query does, so instrumentation that wraps the callback sees it end. Each statement's rows
 * are parsed by pg's Result with the client's type parsers, as for a regular query.
 */
export class BatchQuery extends Query {
  // Set by pg's Query from its config; its typings do not declare it.
  declare callback: ((error: Error | undefined, result: unknown) => void) | undefined
  readonly statements: BatchQueryStatement[]
  readonly promise: Promise<QueryResult[]>
  #first: number
  #count: number
  #results: ResultBuilder[] = []
  #index = 0
  #settled = false

  constructor(statements: BatchQueryStatement[], begin?: string, end?: string) {
    const all = [...statements]
    if (begin) all.unshift({ text: begin })
    if (end) all.push({ text: end })

    super({ text: all.map((statement) => statement.text).join(';\n') })

    this.statements = all
    this.#first = begin ? 1 : 0
    this.#count = statements.length
    this.promise = new Promise((resolve, reject) => {
      this.callback = (error: Error | undefined, results: unknown) =>
        error ? reject(error) : resolve(results as QueryResult[])
    })
  }

  // True once BEGIN has replied (or when there is no BEGIN).
  get beganTransaction(): boolean {
    return this.#index >= this.#first
  }

  // True when every statement has completed, so a failure came from COMMIT or ROLLBACK.
  get reachedEnd(): boolean {
    return this.#index >= this.#first + this.#count
  }

  // Like pg's Query, a value pg cannot send fails the query (the returned error) before
  // anything is written. A property, as pg's typings declare it.
  submit = (pgConnection: Connection): Error | undefined => {
    const values = new Array<unknown[]>(this.statements.length)
    try {
      for (let i = 0; i < this.statements.length; i++) {
        values[i] = this.statements[i].values?.map(prepareValue) ?? noValues
      }
    } catch (error) {
      return error instanceof Error ? error : new Error(String(error))
    }

    // The client sets its type parsers on the Query's own Result before submitting it.
    const types = (this as unknown as { _result: { _types: ResultTypes } })._result._types
    this.#results = this.statements.map(
      () => new Result(undefined as unknown as string, types) as ResultBuilder
    )

    const connection = pgConnection as unknown as BatchQueryConnection
    const stream = connection.stream
    stream.cork()
    try {
      for (let i = 0; i < this.statements.length; i++) {
        connection.parse({ text: this.statements[i].text })
        connection.bind({ values: values[i] })
        connection.describe({ type: 'P', name: '' })
        connection.execute({ portal: '', rows: 0 })
        if (i < this.#first) {
          connection.flush()
        }
      }
      connection.sync()
    } finally {
      stream.uncork()
    }
    return undefined
  }

  handleRowDescription(message: { fields: unknown[] }): void {
    this.#results[this.#index].addFields(message.fields)
  }

  handleDataRow(message: { fields: unknown[] }): void {
    const result = this.#results[this.#index]
    result.addRow(result.parseRow(message.fields))
  }

  handleCommandComplete(message: unknown): void {
    this.#results[this.#index++].addCommandComplete(message)
  }

  handleEmptyQuery(): void {
    this.#index++
  }

  handlePortalSuspended(): void {}

  handleCopyInResponse(connection: Connection): void {
    ;(connection as unknown as BatchQueryConnection).sendCopyFail(
      'COPY is not supported in a batch'
    )
  }

  handleCopyData(): void {}

  // pg forgets the active query on an error, so ReadyForQuery never reaches this one then.
  handleError(error: Error): void {
    if (this.#settled) return
    this.#settled = true
    this.callback?.(error, undefined)
  }

  handleReadyForQuery(): void {
    if (this.#settled) return
    this.#settled = true
    this.callback?.(undefined, this.#results.slice(this.#first, this.#first + this.#count))
  }
}
