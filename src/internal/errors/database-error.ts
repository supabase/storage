import { DatabaseError } from 'pg'

export function isDatabaseSlowDownError(error: Error): boolean {
  return (
    error instanceof DatabaseError &&
    [
      'Authentication error', // supavisor specific
      'Max client connections reached',
      'remaining connection slots are reserved for non-replication superuser connections',
      'no more connections allowed',
      'sorry, too many clients already',
      'server login has been failing, try again later',
      'server login has been failing, cached error: connect timeout (server_login_retry)',
      'server login has been failing, cached error: the database system is not accepting connections (server_login_retry)',
    ].some((msg) => error.message.includes(msg))
  )
}
