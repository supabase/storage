import { isDBMigrationName, isUnrecognizedMigration } from './guards'

describe('migration name guards', () => {
  it.each([undefined, ''])('keeps missing values out of the future-name clamp: %j', (value) => {
    expect(isUnrecognizedMigration(value)).toBe(false)
    expect(isDBMigrationName(value)).toBe(false)
  })

  it('recognizes an applied migration', () => {
    expect(isDBMigrationName('initialmigration')).toBe(true)
    expect(isUnrecognizedMigration('initialmigration')).toBe(false)
  })

  it.each(['future-migration', 'toString'])('treats %s as unrecognized', (value) => {
    expect(isDBMigrationName(value)).toBe(false)
    expect(isUnrecognizedMigration(value)).toBe(true)
  })
})
