import { expectTypeOf } from 'vitest'
import { isDBMigrationName, isUnrecognizedMigration } from './guards'

describe('migration name guards', () => {
  it('keeps recognized names in the false branch', () => {
    const value = 'initialmigration'

    if (!isUnrecognizedMigration(value)) {
      expectTypeOf(value).toEqualTypeOf<string>()
    }
  })

  it.each([
    undefined,
    null,
    '',
    0,
    {},
    [],
  ])('keeps missing or non-string values out of the future-name clamp: %j', (value) => {
    expect(isUnrecognizedMigration(value)).toBe(false)
    expect(isDBMigrationName(value)).toBe(false)
  })

  it.each([
    'initialmigration',
    'objects-key-version-index',
  ])('recognizes the applied migration %s', (value) => {
    expect(isDBMigrationName(value)).toBe(true)
    expect(isUnrecognizedMigration(value)).toBe(false)
  })

  it.each(['future-migration', 'toString', 'constructor'])('treats %s as unrecognized', (value) => {
    expect(isDBMigrationName(value)).toBe(false)
    expect(isUnrecognizedMigration(value)).toBe(true)
  })
})
