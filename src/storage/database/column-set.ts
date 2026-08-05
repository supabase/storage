import { DBMigration } from '@internal/database/migrations/types'
import { quoteIdentifier } from '@internal/database/sql'

type MigrationName = keyof typeof DBMigration
type ProjectionMode = 'physical' | 'synthetic'

const columnSelectionRuntimeKey: unique symbol = Symbol.for('storage.columnSelection')
const columnSetBrand: unique symbol = Symbol('storage.columnSet')
const staticSqlLiteralBrand: unique symbol = Symbol('storage.staticSqlLiteral')

declare const columnSelectionBrand: unique symbol
declare const columnStateBrand: unique symbol

export type ColumnName<Row> = Extract<keyof Row, string>
type Invariant<Value> = (value: Value) => Value

// An immutable, precompiled SQL projection token for one row family.
export interface ColumnSelection<Row, SetId extends symbol> {
  readonly [columnSelectionBrand]: Invariant<readonly [Row, SetId]>
}

export type ColumnState<Row, SetId extends symbol> = number & {
  readonly [columnStateBrand]: Invariant<readonly [Row, SetId]>
}

// Selects named projection tokens at module initialization.
export interface ColumnSet<Row, Column extends string, SetId extends symbol> {
  select(...columns: readonly [Column | '*', ...(Column | '*')[]]): ColumnSelection<Row, SetId>
}

// Derives the opaque state without exposing a set's identity token.
export type ColumnSetState<Set> =
  Set extends ColumnSet<infer Row, infer _Column extends string, infer SetId extends symbol>
    ? ColumnState<Row, SetId>
    : never

export interface StaticSqlLiteral {
  readonly [staticSqlLiteralBrand]: true
}

export interface ColumnSetDefinition<Column extends string> {
  // Baseline column selected when compatibility filtering removes everything requested.
  readonly fallback: Column
  // Exceptional columns that do not exist before the named migration.
  readonly availableFrom?: Readonly<Partial<Record<Column, MigrationName>>>
  // Encoded values that replace requested columns in synthetic mode.
  readonly synthetic?: Readonly<Partial<Record<Column, StaticSqlLiteral>>>
}

interface CompiledStaticSqlLiteral extends StaticSqlLiteral {
  readonly sql: string
}

interface CompiledColumn {
  readonly name: string
  readonly sql: string
}

interface ColumnSetRuntime {
  readonly thresholds: readonly number[]
  readonly availableEpoch: Readonly<Record<string, number>>
  readonly syntheticSql: Readonly<Record<string, string>>
  readonly epochCount: number
  readonly syntheticOffset?: number
  readonly fallbackSql: string
}

interface CompiledColumnSet<Row, Column extends string, SetId extends symbol>
  extends ColumnSet<Row, Column, SetId> {
  readonly [columnSetBrand]: ColumnSetRuntime
}

interface CompiledColumnSelection {
  readonly [columnSelectionRuntimeKey]: readonly string[]
}

// Encodes a string value so table policies never accept arbitrary SQL fragments.
export function staticSqlLiteral(value: string): StaticSqlLiteral {
  return Object.freeze({
    [staticSqlLiteralBrand]: true as const,
    sql: `'${value.replace(/'/g, "''")}'`,
  })
}

export function defineColumnSet<Row, SetId extends symbol>(
  definition: ColumnSetDefinition<ColumnName<Row>>
): ColumnSet<Row, ColumnName<Row>, SetId> {
  const fallbackSql = quoteIdentifier(definition.fallback)
  const availableFrom: Record<string, number> = Object.create(null)
  const thresholdSet = new Set<number>()

  for (const [column, migration] of Object.entries(definition.availableFrom ?? {})) {
    quoteIdentifier(column)

    const ordinal = DBMigration[migration as MigrationName]
    if (typeof ordinal !== 'number') {
      throw new Error(`Unknown database migration: ${String(migration)}`)
    }

    availableFrom[column] = ordinal
    thresholdSet.add(ordinal)
  }

  if (availableFrom[definition.fallback] !== undefined) {
    throw new Error(`Fallback column must be available in every migration: ${definition.fallback}`)
  }

  const thresholds = Object.freeze(Array.from(thresholdSet).sort((left, right) => left - right))
  const availableEpoch: Record<string, number> = Object.create(null)

  for (const [column, ordinal] of Object.entries(availableFrom)) {
    availableEpoch[column] = thresholds.indexOf(ordinal) + 1
  }

  const syntheticSql: Record<string, string> = Object.create(null)
  for (const [column, literal] of Object.entries(definition.synthetic ?? {})) {
    const quotedColumn = quoteIdentifier(column)

    if (!isStaticSqlLiteral(literal)) {
      throw new Error(`Invalid static SQL literal for column: ${column}`)
    }

    syntheticSql[column] = `${literal.sql} AS ${quotedColumn}`
  }

  const epochCount = thresholds.length + 1
  const hasSyntheticColumns = Object.keys(syntheticSql).length > 0
  const runtime: ColumnSetRuntime = Object.freeze({
    thresholds,
    availableEpoch: Object.freeze(availableEpoch),
    syntheticSql: Object.freeze(syntheticSql),
    epochCount,
    syntheticOffset: hasSyntheticColumns ? epochCount : undefined,
    fallbackSql,
  })

  function compileSelection(columns: readonly string[]): ColumnSelection<Row, SetId> {
    const compiledColumns = compileColumns(columns)

    const variants = new Array<string>(
      runtime.epochCount * (runtime.syntheticOffset === undefined ? 1 : 2)
    )

    for (let epoch = 0; epoch < runtime.epochCount; epoch++) {
      variants[epoch] = compileVariant(compiledColumns, runtime, epoch, 'physical')

      if (runtime.syntheticOffset !== undefined) {
        variants[runtime.syntheticOffset + epoch] = compileVariant(
          compiledColumns,
          runtime,
          epoch,
          'synthetic'
        )
      }
    }

    return Object.freeze({
      [columnSelectionRuntimeKey]: Object.freeze(variants),
    }) as unknown as ColumnSelection<Row, SetId>
  }

  const set: CompiledColumnSet<Row, ColumnName<Row>, SetId> = {
    select(...columns) {
      return compileSelection(columns)
    },
    [columnSetBrand]: runtime,
  }

  return Object.freeze(set)
}

// Maps a verified migration ordinal to a compact table state outside the query path.
export function prepareColumnState<Row, Column extends string, SetId extends symbol>(
  set: ColumnSet<Row, Column, SetId>,
  migrationOrdinal: number,
  mode: ProjectionMode = 'physical'
): ColumnState<Row, SetId> {
  if (!Number.isSafeInteger(migrationOrdinal) || migrationOrdinal < 0) {
    throw new Error(`Invalid database migration ordinal: ${migrationOrdinal}`)
  }

  const runtime = (set as CompiledColumnSet<Row, Column, SetId>)[columnSetBrand]
  let epoch = 0

  while (epoch < runtime.thresholds.length && migrationOrdinal >= runtime.thresholds[epoch]) {
    epoch++
  }

  let state = epoch

  if (mode === 'synthetic' && runtime.syntheticOffset !== undefined) {
    state += runtime.syntheticOffset
  }

  return state as ColumnState<Row, SetId>
}

// Hot path: one symbol-property read and one array lookup.
export function resolveColumns<Row, SetId extends symbol>(
  selection: ColumnSelection<Row, SetId>,
  state: ColumnState<Row, SetId>
): string {
  return (selection as unknown as CompiledColumnSelection)[columnSelectionRuntimeKey][state]
}

function compileVariant(
  columns: readonly CompiledColumn[],
  runtime: ColumnSetRuntime,
  epoch: number,
  mode: ProjectionMode
): string {
  if (columns.length === 1 && columns[0].name === '*') {
    return '*'
  }

  const physical: string[] = []
  const synthetic: string[] = []

  for (const column of columns) {
    const syntheticColumn = mode === 'synthetic' ? runtime.syntheticSql[column.name] : undefined
    if (syntheticColumn !== undefined) {
      synthetic.push(syntheticColumn)
      continue
    }

    const availableEpoch = runtime.availableEpoch[column.name]
    if (availableEpoch !== undefined && epoch < availableEpoch) {
      continue
    }

    physical.push(column.sql)
  }

  if (physical.length === 0 && synthetic.length === 0) {
    return runtime.fallbackSql
  }

  return physical.concat(synthetic).join(', ')
}

function compileColumns(columns: readonly string[]): readonly CompiledColumn[] {
  if (columns.length === 0) {
    throw new Error('Column selection cannot be empty')
  }

  const seen = new Set<string>()
  return columns.map((column) => {
    if (seen.has(column)) {
      throw new Error(`Duplicate column in selection: ${column}`)
    }
    seen.add(column)

    if (column === '*') {
      if (columns.length !== 1) {
        throw new Error('Wildcard cannot be combined with named columns')
      }
      return { name: column, sql: column }
    }

    return { name: column, sql: quoteIdentifier(column) }
  })
}

function isStaticSqlLiteral(value: unknown): value is CompiledStaticSqlLiteral {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as Partial<CompiledStaticSqlLiteral>)[staticSqlLiteralBrand] === true &&
    typeof (value as Partial<CompiledStaticSqlLiteral>).sql === 'string'
  )
}
