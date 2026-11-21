import { randomUUID } from 'node:crypto'
import { withOptionalVersion } from '@storage/backend/adapter'
import { newContinuation } from '../../test/utils/lifecycle-continuation'
import {
  advanceEmptyLifecyclePage,
  advanceLifecycleBatch,
  decodeLifecycleContinuation,
  freezeLifecycleBatchVersion,
  LifecycleContinuationDecodeError,
} from './continuation'

function validContinuation() {
  return {
    ...newContinuation(randomUUID(), { snapshotAt: '2026-08-06T00:00:00.000Z' }),
    cursor: { name: 'logs/example.json', archivedAt: '2026-08-05T12:00:00.000Z' },
  }
}

describe('decodeLifecycleContinuation', () => {
  test('decodes a v1 continuation', () => {
    const input = validContinuation()
    expect(decodeLifecycleContinuation(input)).toEqual(input)
  })

  test('rejects a continuation that still carries a batch', () => {
    expect(() =>
      decodeLifecycleContinuation({ ...validContinuation(), batch: { versions: [] } })
    ).toThrow(LifecycleContinuationDecodeError)
  })

  test.each([2, 3])(
    'fails closed on unknown continuation version %s before reading the payload',
    (continuationVersion) => {
      expect(() => decodeLifecycleContinuation({ continuationVersion })).toThrow(
        `Unsupported lifecycle continuation version ${continuationVersion}`
      )
    }
  )

  test.each([
    [
      'invalid UUID',
      (value: ReturnType<typeof validContinuation>) => {
        value.runId = 'not-a-uuid'
      },
    ],
    [
      'invalid snapshot timestamp',
      (value: ReturnType<typeof validContinuation>) => {
        value.snapshotAt = 'yesterday'
      },
    ],
    [
      'negative counter',
      (value: ReturnType<typeof validContinuation>) => {
        value.counters.versionsExamined = -1
      },
    ],
  ])('rejects %s', (_name, mutate) => {
    const value = validContinuation()
    mutate(value)
    expect(() => decodeLifecycleContinuation(value)).toThrow(LifecycleContinuationDecodeError)
  })
})

describe('lifecycle continuation transitions', () => {
  test('advances pages without eligible candidates and rejects pages with candidates', () => {
    const initial = newContinuation(randomUUID(), { snapshotAt: '2026-08-15T00:00:00.000Z' })
    const page = {
      rawRowsExamined: 1,
      candidates: [],
      pageEnd: { name: 'logs/example.json', archivedAt: '2026-08-02T00:00:00.000Z' },
      exhausted: false,
    }

    expect(advanceEmptyLifecyclePage(initial, page)).toEqual({
      ...initial,
      cursor: page.pageEnd,
      counters: { ...initial.counters, versionsExamined: 1 },
    })

    expect(() =>
      advanceEmptyLifecyclePage(initial, {
        ...page,
        candidates: [
          {
            id: randomUUID(),
            name: page.pageEnd.name,
            version: randomUUID(),
            isDeleteMarker: false,
            archivedAt: page.pageEnd.archivedAt,
            rawRowNumber: 1,
          },
        ],
      })
    ).toThrow('Lifecycle pages with eligible candidates must be armed before advancing')
  })

  test('advances a batch to its page end with examined, eligible and deleted counts', () => {
    const initial = newContinuation(randomUUID(), { snapshotAt: '2026-08-15T00:00:00.000Z' })
    const versions = [0, 1].map(() =>
      freezeLifecycleBatchVersion({
        id: randomUUID(),
        name: 'logs/example.json',
        version: randomUUID(),
        isDeleteMarker: false,
      })
    )
    const pageEnd = { name: 'logs/example.json', archivedAt: '2026-08-02T00:00:00.000Z' }
    const batch = { pageEnd, rawRowsExamined: 5, versions }

    expect(
      advanceLifecycleBatch(initial, batch, { objectVersions: 1, deleteMarkers: 0, bytes: 7 })
    ).toEqual({
      ...initial,
      cursor: pageEnd,
      counters: {
        versionsExamined: 5,
        versionsEligible: 2,
        objectVersionsDeleted: 1,
        deleteMarkersDeleted: 0,
        bytesDeleted: 7,
        batchesCompleted: 1,
      },
    })
    expect(
      advanceLifecycleBatch(initial, batch, { objectVersions: 0, deleteMarkers: 0, bytes: 0 })
        .counters.batchesCompleted
    ).toBe(0)
  })

  test('saturates the cumulative byte counter at the safe integer limit', () => {
    const initial = validContinuation()
    initial.counters.bytesDeleted = Number.MAX_SAFE_INTEGER - 1
    const pageEnd = { name: 'a', archivedAt: '2026-08-02T00:00:00.000Z' }
    expect(
      advanceLifecycleBatch(
        initial,
        { pageEnd, rawRowsExamined: 1, versions: [] },
        { objectVersions: 1, deleteMarkers: 0, bytes: 10 }
      ).counters.bytesDeleted
    ).toBe(Number.MAX_SAFE_INTEGER)
  })

  test('freezes exact physical keys using the backend locator helper', () => {
    const physicalKey = withOptionalVersion('logs/example.json', 'version-a')
    const id = randomUUID()
    expect(
      freezeLifecycleBatchVersion({
        id,
        name: 'logs/example.json',
        version: 'version-a',
        isDeleteMarker: false,
      })
    ).toEqual({
      id,
      name: 'logs/example.json',
      version: 'version-a',
      artifacts: [physicalKey, `${physicalKey}.info`],
    })

    expect(
      freezeLifecycleBatchVersion({
        id: randomUUID(),
        name: 'deleted',
        version: randomUUID(),
        isDeleteMarker: true,
      }).artifacts
    ).toEqual([])

    expect(
      freezeLifecycleBatchVersion({
        id: randomUUID(),
        name: 'logs/example.json',
        version: null,
        isDeleteMarker: false,
      }).artifacts
    ).toEqual(['logs/example.json'])
  })
})
