import { Knex } from 'knex'
import { createHmac, timingSafeEqual } from 'node:crypto'
import { getConfig } from '../../config'

const { eventLogSigningKey } = getConfig()

export interface EventLogEntry {
  eventName: string
  payload: Record<string, unknown>
  sendOptions?: Record<string, unknown>
}

export interface EventTransaction {
  tenantId: string
  insertEventLog(event: EventLogEntry): Promise<void>
}

/**
 * Computes an HMAC-SHA256 signature over the canonical representation of an event log entry.
 * The canonical form is: event_name + '.' + JSON-stringified payload + '.' + JSON-stringified send_options
 */
export function computeEventLogSignature(event: EventLogEntry): string {
  const canonical =
    event.eventName +
    '.' +
    JSON.stringify(event.payload) +
    '.' +
    (event.sendOptions ? JSON.stringify(event.sendOptions) : '')

  return createHmac('sha256', eventLogSigningKey).update(canonical).digest('hex')
}

/**
 * Verifies that an event log row's signature matches its contents.
 */
export function verifyEventLogSignature(
  eventName: string,
  payload: Record<string, unknown>,
  sendOptions: Record<string, unknown> | null,
  signature: string
): boolean {
  const canonical =
    eventName +
    '.' +
    JSON.stringify(payload) +
    '.' +
    (sendOptions ? JSON.stringify(sendOptions) : '')

  const expected = createHmac('sha256', eventLogSigningKey).update(canonical).digest()
  const actual = Buffer.from(signature, 'hex')

  if (expected.length !== actual.length) {
    return false
  }

  return timingSafeEqual(expected, actual)
}

export class KnexEventTransaction implements EventTransaction {
  constructor(private readonly knex: Knex, public readonly tenantId: string) {}

  async insertEventLog(event: EventLogEntry): Promise<void> {
    const signature = computeEventLogSignature(event)

    await this.knex('event_log')
      .withSchema('storage')
      .insert({
        event_name: event.eventName,
        payload: JSON.stringify(event.payload),
        send_options: event.sendOptions ? JSON.stringify(event.sendOptions) : null,
        signature,
      })
  }
}
