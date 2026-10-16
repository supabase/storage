import { randomUUID } from 'node:crypto'
import { withOptionalVersion } from '@storage/backend/adapter'
import { newContinuation } from '../../test/utils/lifecycle-continuation'
import {
  advanceEmptyLifecyclePage,
  armLifecycleAttempt,
  commitLifecycleBatchResults,
  decodeLifecycleContinuation,
  filterStagedLifecycleBatch,
  freezeLifecycleBatchVersion,
  LifecycleContinuationDecodeError,
  recordLifecycleArtifactOutcomes,
  stageLifecycleBatch,
} from './continuation'

const noDeletions = { objectVersions: 0, deleteMarkers: 0, bytes: 0 }

function validContinuation() {
  const generation = randomUUID()
  return {
    ...newContinuation(generation, { snapshotAt: '2026-08-06T00:00:00.000Z' }),
    pageEnd: {
      name: 'logs/example.json',
      archivedAt: '2026-08-05T12:00:00.000Z',
    },
    counters: {
      versionsExamined: 1,
      versionsEligible: 1,
      objectVersionsDeleted: 0,
      deleteMarkersDeleted: 0,
      bytesDeleted: 0,
      batchesCompleted: 0,
    },
    batch: {
      versions: [
        {
          id: randomUUID(),
          name: 'logs/example.json',
          version: randomUUID(),
          artifacts: [
            { key: 'logs/example.json/version', outcome: 'UNRESOLVED' },
            { key: 'logs/example.json/version.info', outcome: 'UNRESOLVED' },
          ],
        },
      ],
      inFlight: {
        attemptId: randomUUID(),
        startedAt: '2026-08-06T00:01:00.000Z',
      },
    },
  }
}

describe('decodeLifecycleContinuation', () => {
  test('decodes a bounded v1 continuation', () => {
    const input = validContinuation()
    expect(decodeLifecycleContinuation(input)).toEqual(input)
  })

  test('decodes stored v1 journals with the retired trigger and topology fields', () => {
    const input = validContinuation()
    expect(
      decodeLifecycleContinuation({
        ...input,
        trigger: 'scheduled',
        topology: { scanKind: 'NONCURRENT', epoch: '1', shardId: 0, shardCount: 1 },
      })
    ).toEqual(input)
  })

  test('rejects a resolved artifact with a missing companion', () => {
    const input = validContinuation()
    const artifacts = input.batch.versions[0].artifacts
    artifacts[0].outcome = 'DELETED'
    artifacts.pop()

    expect(() => decodeLifecycleContinuation(input)).toThrow(
      'batch.versions[0].artifacts must contain 0 or 2 entries'
    )
  })

  test('round-trips an armed legacy physical version without replacing null with a sentinel', () => {
    const input = validContinuation()
    const legacy = freezeLifecycleBatchVersion({
      id: randomUUID(),
      name: 'logs/example.json',
      version: null,
      isDeleteMarker: false,
    })
    const journal = { ...input, batch: { ...input.batch, versions: [legacy] } }
    expect(decodeLifecycleContinuation(JSON.parse(JSON.stringify(journal)))).toEqual(journal)
    expect(legacy.artifacts).toEqual([{ key: 'logs/example.json', outcome: 'UNRESOLVED' }])
  })

  test('rejects an omitted physical version selector in a durable batch', () => {
    const input = validContinuation()
    const { version: _version, ...entry } = input.batch.versions[0]
    expect(() =>
      decodeLifecycleContinuation({
        ...input,
        batch: { ...input.batch, versions: [{ ...entry, artifacts: entry.artifacts.slice(0, 1) }] },
      })
    ).toThrow('batch.versions[0].version')
  })

  test.each([
    2, 3,
  ])('fails closed on unknown continuation version %s before reading the payload', (continuationVersion) => {
    expect(() => decodeLifecycleContinuation({ continuationVersion })).toThrow(
      `Unsupported lifecycle continuation version ${continuationVersion}`
    )
  })

  test.each([
    [
      'invalid UUID',
      (value: ReturnType<typeof validContinuation>) => {
        value.runId = 'not-a-uuid' as ReturnType<typeof validContinuation>['runId']
      },
    ],
    [
      'batch without page end',
      (value: ReturnType<typeof validContinuation>) => {
        delete (value as { pageEnd?: unknown }).pageEnd
      },
    ],
    [
      'unknown artifact outcome',
      (value: ReturnType<typeof validContinuation>) => {
        value.batch.versions[0].artifacts[0].outcome = 'MAYBE'
      },
    ],
  ])('rejects %s', (_name, mutate) => {
    const value = validContinuation()
    mutate(value)
    expect(() => decodeLifecycleContinuation(value)).toThrow(LifecycleContinuationDecodeError)
  })

  test('bounds the durable logical-version batch', () => {
    const value = validContinuation()
    value.batch.versions = Array.from({ length: 501 }, () => value.batch.versions[0])
    expect(() => decodeLifecycleContinuation(value)).toThrow('at most 500 entries')
  })
})

describe('lifecycle continuation transitions', () => {
  test('rejects empty staging and advances pages without eligible candidates', () => {
    const initial = newContinuation(randomUUID(), { snapshotAt: '2026-08-15T00:00:00.000Z' })
    const page = {
      rawRowsExamined: 1,
      candidates: [],
      pageEnd: { name: 'logs/example.json', archivedAt: '2026-08-02T00:00:00.000Z' },
      exhausted: false,
    }

    expect(() =>
      stageLifecycleBatch(initial, {
        pageEnd: page.pageEnd,
        rawRowsExamined: page.rawRowsExamined,
        versions: [],
      })
    ).toThrow('Lifecycle batch cannot be empty')

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
          },
        ],
      })
    ).toThrow('Lifecycle pages with eligible candidates must be staged before advancing')
  })

  test('keeps null and literal null-string identities separate through filtering and recovery', () => {
    const initial = validContinuation()
    const legacy = freezeLifecycleBatchVersion({
      id: randomUUID(),
      name: 'logs/example.json',
      version: null,
      isDeleteMarker: false,
    })
    const named = freezeLifecycleBatchVersion({
      id: randomUUID(),
      name: legacy.name,
      version: 'null',
      isDeleteMarker: false,
    })
    const staged = decodeLifecycleContinuation({
      ...initial,
      counters: { ...initial.counters, versionsExamined: 2, versionsEligible: 2 },
      batch: { versions: [legacy, named] },
    })

    expect(filterStagedLifecycleBatch(staged, [legacy]).batch?.versions).toEqual([legacy])
    expect(filterStagedLifecycleBatch(staged, [named]).batch?.versions).toEqual([named])

    const armed = armLifecycleAttempt(staged, initial.batch.inFlight)
    const recorded = recordLifecycleArtifactOutcomes(
      armed,
      new Map(named.artifacts.map(({ key }) => [key, { outcome: 'ABSENT' as const }]))
    )
    const recovered = commitLifecycleBatchResults(recorded, {
      objectVersions: 1,
      deleteMarkers: 0,
      bytes: 12,
    })
    expect(recovered.batch).toEqual({ versions: [legacy], inFlight: initial.batch.inFlight })
    expect(recovered.cursor).toBeUndefined()
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
      artifacts: [
        { key: physicalKey, outcome: 'UNRESOLVED' },
        { key: `${physicalKey}.info`, outcome: 'UNRESOLVED' },
      ],
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
    ).toEqual([{ key: 'logs/example.json', outcome: 'UNRESOLVED' }])
  })

  test('requires an armed attempt to complete a delete-marker batch', () => {
    const initial = validContinuation()
    const marker = freezeLifecycleBatchVersion({
      id: randomUUID(),
      name: initial.pageEnd.name,
      version: randomUUID(),
      isDeleteMarker: true,
    })
    const staged = decodeLifecycleContinuation({
      ...initial,
      batch: { versions: [marker] },
    })

    expect(() => commitLifecycleBatchResults(staged, noDeletions)).toThrow(
      'must be armed before committing'
    )

    const armed = armLifecycleAttempt(staged, initial.batch.inFlight)
    const completed = commitLifecycleBatchResults(armed, {
      objectVersions: 0,
      deleteMarkers: 1,
      bytes: 0,
    })
    expect(completed.cursor).toEqual(staged.pageEnd)
    expect(completed.batch).toBeUndefined()
    expect(completed.counters).toEqual({
      ...staged.counters,
      deleteMarkersDeleted: 1,
      batchesCompleted: 1,
    })

    const filtered = filterStagedLifecycleBatch(staged, [])
    expect(filtered.cursor).toEqual(staged.pageEnd)
    expect(filtered.batch).toBeUndefined()
    expect(filtered.counters).toEqual({ ...staged.counters, batchesCompleted: 1 })
  })

  test('keeps one stable attempt as compensation authority across generation replacement', () => {
    const generation = randomUUID()
    const initial = newContinuation(generation, { snapshotAt: '2026-08-15T00:00:00.000Z' })
    const candidate = freezeLifecycleBatchVersion({
      id: randomUUID(),
      name: 'logs/example.json',
      version: randomUUID(),
      isDeleteMarker: false,
    })
    const staged = stageLifecycleBatch(initial, {
      pageEnd: { name: candidate.name, archivedAt: '2026-08-02T00:00:00.000Z' },
      rawRowsExamined: 1,
      versions: [candidate],
    })
    const attempt = {
      attemptId: randomUUID(),
      startedAt: '2026-08-15T00:01:00.000Z',
    }
    const armed = armLifecycleAttempt(staged, attempt)

    expect(commitLifecycleBatchResults(armed, noDeletions).batch?.versions).toHaveLength(1)

    const partiallyResolved = recordLifecycleArtifactOutcomes(
      armed,
      new Map([[candidate.artifacts[0].key, { outcome: 'DELETED' }]])
    )
    expect(partiallyResolved.batch?.inFlight).toEqual(attempt)
    expect(
      commitLifecycleBatchResults(partiallyResolved, noDeletions).batch?.versions
    ).toHaveLength(1)

    const resolved = recordLifecycleArtifactOutcomes(
      partiallyResolved,
      new Map([[candidate.artifacts[1].key, { outcome: 'ABSENT' }]])
    )
    expect(commitLifecycleBatchResults(resolved, noDeletions)).toMatchObject({
      cursor: staged.pageEnd,
      counters: { versionsExamined: 1, versionsEligible: 1, batchesCompleted: 1 },
    })
  })

  test('rejects replacing an armed attempt', () => {
    const continuation = decodeLifecycleContinuation(validContinuation())
    expect(() =>
      armLifecycleAttempt(continuation, {
        attemptId: randomUUID(),
        startedAt: '2026-08-15T00:01:00.000Z',
      })
    ).toThrow('already has an armed attempt')
  })

  test('retains partial artifact success and records completed-version counters immediately', () => {
    const continuation = decodeLifecycleContinuation(validContinuation())
    const version = continuation.batch!.versions[0]
    const partial = recordLifecycleArtifactOutcomes(
      continuation,
      new Map([
        [version.artifacts[0].key, { outcome: 'DELETED' as const }],
        [version.artifacts[1].key, { outcome: 'FAILED' as const, error: 'retry me' }],
      ])
    )

    expect(
      commitLifecycleBatchResults(partial, {
        objectVersions: 2,
        deleteMarkers: 1,
        bytes: 30,
      })
    ).toMatchObject({
      batch: {
        inFlight: partial.batch!.inFlight,
        versions: [
          {
            name: version.name,
            version: version.version,
            artifacts: [
              expect.objectContaining({ outcome: 'DELETED' }),
              expect.objectContaining({ outcome: 'FAILED' }),
            ],
          },
        ],
      },
      counters: {
        objectVersionsDeleted: 2,
        deleteMarkersDeleted: 1,
        bytesDeleted: 30,
        batchesCompleted: 0,
      },
    })
  })

  test('saturates the cumulative byte counter at the safe integer limit', () => {
    const input = validContinuation()
    input.counters.bytesDeleted = Number.MAX_SAFE_INTEGER - 1
    const continuation = decodeLifecycleContinuation(input)
    expect(
      commitLifecycleBatchResults(continuation, {
        objectVersions: 1,
        deleteMarkers: 0,
        bytes: 10,
      }).counters.bytesDeleted
    ).toBe(Number.MAX_SAFE_INTEGER)
  })

  test('drains definitive all-failed versions', () => {
    const armed = decodeLifecycleContinuation(validContinuation())
    const failed = recordLifecycleArtifactOutcomes(
      armed,
      new Map(
        armed.batch!.versions[0].artifacts.map((artifact) => [
          artifact.key,
          { outcome: 'FAILED' as const, error: 'not deleted' },
        ])
      )
    )
    expect(commitLifecycleBatchResults(failed, noDeletions)).toMatchObject({
      cursor: armed.pageEnd,
      counters: { batchesCompleted: 1 },
    })
  })
})
