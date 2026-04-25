import {
  buildQueueOverflowWhereClause,
  normalizeQueueOverflowFilters,
  parseQueueOverflowCsv,
} from './overflow'

describe('parseQueueOverflowCsv', () => {
  it('returns undefined for empty input', () => {
    expect(parseQueueOverflowCsv(undefined)).toBeUndefined()
    expect(parseQueueOverflowCsv(' , , ')).toBeUndefined()
  })

  it('trims, de-duplicates, and preserves order', () => {
    expect(
      parseQueueOverflowCsv(' ObjectRemoved:Delete, ObjectCreated:Put,ObjectRemoved:Delete ')
    ).toEqual(['ObjectRemoved:Delete', 'ObjectCreated:Put'])
  })
})

describe('normalizeQueueOverflowFilters', () => {
  it('trims values and removes empty strings', () => {
    expect(
      normalizeQueueOverflowFilters({
        name: ' webhooks ',
        eventTypes: [' ObjectRemoved:Delete ', ''],
        tenantRefs: [' tenant-a ', 'tenant-a', '   '],
      })
    ).toEqual({
      name: 'webhooks',
      eventTypes: ['ObjectRemoved:Delete'],
      tenantRefs: ['tenant-a'],
    })
  })
})

describe('buildQueueOverflowWhereClause', () => {
  it('always scopes queries to created jobs', () => {
    expect(buildQueueOverflowWhereClause({})).toEqual({
      sql: 'state = ?',
      bindings: ['created'],
    })
  })

  it('adds queue, event-type, and tenant filters in a stable order', () => {
    expect(
      buildQueueOverflowWhereClause({
        name: ' webhooks ',
        eventTypes: ['ObjectRemoved:Delete', ' ObjectCreated:Put '],
        tenantRefs: ['tenant-b', 'tenant-a'],
      })
    ).toEqual({
      sql: "state = ? AND name = ? AND data->'event'->>'type' IN (?, ?) AND data->'tenant'->>'ref' IN (?, ?)",
      bindings: [
        'created',
        'webhooks',
        'ObjectRemoved:Delete',
        'ObjectCreated:Put',
        'tenant-b',
        'tenant-a',
      ],
    })
  })
})
