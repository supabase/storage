import { DBMigration } from './types'

export function isDBMigrationName(value: unknown): value is keyof typeof DBMigration {
  return typeof value === 'string' && Object.prototype.hasOwnProperty.call(DBMigration, value)
}

/** A non-empty name this binary does not know, e.g. one recorded by a newer release. */
export function isUnrecognizedMigration(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && !isDBMigrationName(value)
}

const knownMigrationNames = Object.keys(DBMigration) as (keyof typeof DBMigration)[]
const highestKnownMigration = knownMigrationNames.reduce((highest, name) =>
  DBMigration[name] > DBMigration[highest] ? name : highest
)

export function knownMigrationVersions(): string[] {
  return knownMigrationNames
}

/** Highest migration this binary knows, ignoring any freeze target. */
export function highestKnownMigrationName() {
  return highestKnownMigration
}
