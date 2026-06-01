import { DatabaseError } from 'pg'
import { isDatabaseSlowDownError } from './database-error'

function databaseError(message: string): DatabaseError {
  return new DatabaseError(message, message.length, 'error')
}

describe('isDatabaseSlowDownError', () => {
  it('matches cached server login failures with provider-specific suffixes', () => {
    const messages = [
      'server login has been failing, cached error: connect timeout (server_login_retry)',
      'server login has been failing, cached error: the database system is not accepting connections',
      'server login has been failing, cached error: pgbouncer cannot connect to server',
    ]

    for (const message of messages) {
      expect(isDatabaseSlowDownError(databaseError(message))).toBe(true)
    }
  })

  it('does not match unrelated database errors', () => {
    const messages = [
      'relation "objects" does not exist',
      'duplicate key value violates unique constraint "objects_pkey"',
      'syntax error at or near "from"',
    ]

    for (const message of messages) {
      expect(isDatabaseSlowDownError(databaseError(message))).toBe(false)
    }
  })

  it('does not match slowdown text on non-database errors', () => {
    expect(
      isDatabaseSlowDownError(
        new Error('server login has been failing, cached error: connect timeout')
      )
    ).toBe(false)
  })
})
