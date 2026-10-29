import { randomUUID } from 'node:crypto'
import { withOptionalVersion } from '@storage/backend/adapter'
import {
  armedContinuation,
  newContinuation,
  stagedContinuation,
} from '../../test/utils/lifecycle-continuation'
import {
  advanceEmptyLifecyclePage,
  armLifecycleAttempt,
  commitLifecycleBatchResults,
  decodeLifecycleContinuation,
  encodeLifecycleContinuation,
  freezeLifecycleBatchVersion,
  LifecycleContinuationDecodeError,
  recordLifecycleArtifactOutcomes,
  recordLifecycleMetadataDeletions,
  skipLifecycleBatch,
  stageLifecycleBatch,
} from './continuation'

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
      keySeparator: '/' as const,
      versions: [
        {
          id: randomUUID(),
          name: 'logs/example.json',
          version: 'version',
          artifacts: [{ outcome: 'UNRESOLVED' }, { outcome: 'UNRESOLVED' }],
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
    const decoded = decodeLifecycleContinuation(input)
    expect(decoded.batch?.versions[0].artifacts).toEqual([
      { key: 'logs/example.json/version', outcome: 'UNRESOLVED' },
      { key: 'logs/example.json/version.info', outcome: 'UNRESOLVED' },
    ])
    expect(decoded).toMatchObject({ ...input, batch: { inFlight: input.batch.inFlight } })
  })

  test('ignores a stored artifact key and derives it from the name and version', () => {
    const input = validContinuation()
    input.batch.versions[0].artifacts[0] = { key: 'tampered', outcome: 'UNRESOLVED' } as never
    expect(decodeLifecycleContinuation(input).batch?.versions[0].artifacts[0].key).toBe(
      'logs/example.json/version'
    )
  })

  test('encodes without artifact keys and decodes back to the same continuation', () => {
    const decoded = decodeLifecycleContinuation(validContinuation())
    const encoded = JSON.parse(JSON.stringify(encodeLifecycleContinuation(decoded)))
    expect(encoded.batch.versions[0].artifacts).toEqual([
      { outcome: 'UNRESOLVED' },
      { outcome: 'UNRESOLVED' },
    ])
    expect(decodeLifecycleContinuation(encoded)).toEqual(decoded)
  })

  test('recovers keys using the separator frozen in the journal', () => {
    const input = validContinuation()
    const encoded = { ...input, batch: { ...input.batch, keySeparator: '-$v-' } }
    const recovered = decodeLifecycleContinuation(encoded)
    expect(recovered.batch?.versions[0].artifacts.map(({ key }) => key)).toEqual([
      'logs/example.json-$v-version',
      'logs/example.json-$v-version.info',
    ])
    expect(decodeLifecycleContinuation(encodeLifecycleContinuation(recovered))).toEqual(recovered)
    expect(() =>
      decodeLifecycleContinuation({ ...input, batch: { ...input.batch, keySeparator: undefined } })
    ).toThrow('batch.keySeparator')
  })

  test('encodes a continuation without a batch unchanged', () => {
    const { batch: _batch, ...input } = validContinuation()
    const decoded = decodeLifecycleContinuation(input)
    expect(encodeLifecycleContinuation(decoded)).toEqual(decoded)
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

  test.each([
    '00000000-0000-0000-0000-000000000000',
    '6ba7b810-9dad-31d1-70b4-00c04fd430c8',
  ] as const)('decodes the database uuid %s as a batch version id', (id) => {
    const input = validContinuation()
    input.batch.versions[0].id = id
    expect(decodeLifecycleContinuation(input).batch?.versions[0].id).toBe(id)
  })

  test.each([
    '6ba7b810-9dad-31d1-70b4-00c04fd430c',
    '6ba7b810-9dad-31d1-70b4-00c04fd430cg',
  ] as const)('rejects the malformed batch version id %s', (id) => {
    const input = validContinuation()
    input.batch.versions[0].id = id
    expect(() => decodeLifecycleContinuation(input)).toThrow('batch.versions[0].id must be a UUID')
  })

  test('round-trips every journal shape through the encoded JSON form', () => {
    const pageEnd = { name: 'a/b', archivedAt: '2026-08-05T12:00:00.000Z' }
    const versions = [
      { id: randomUUID(), name: 'a/b', version: randomUUID(), isDeleteMarker: false },
      { id: randomUUID(), name: 'a/b', version: null, isDeleteMarker: false },
      { id: randomUUID(), name: 'a/b', version: 'null', isDeleteMarker: false },
      { id: randomUUID(), name: 'd/-$v-x.info/é', version: randomUUID(), isDeleteMarker: false },
      { id: randomUUID(), name: 'a/b', version: randomUUID(), isDeleteMarker: true },
    ]
    const armed = armedContinuation(randomUUID(), versions, pageEnd)
    expect(armed.batch!.versions[1]).toMatchObject({
      version: null,
      artifacts: [{ key: 'a/b', outcome: 'UNRESOLVED' }],
    })
    const [data, info] = armed.batch!.versions[0].artifacts
    const recorded = recordLifecycleArtifactOutcomes(
      armed,
      new Map([
        [data.key, { outcome: 'UNRESOLVED' as const, error: 'timeout' }],
        [info.key, { outcome: 'FAILED' as const, error: 'denied' }],
        [armed.batch!.versions[1].artifacts[0].key, { outcome: 'DELETED' as const }],
        [armed.batch!.versions[2].artifacts[0].key, { outcome: 'ABSENT' as const }],
      ])
    )
    for (const continuation of [
      newContinuation(randomUUID()),
      { ...newContinuation(randomUUID()), cursor: pageEnd },
      stagedContinuation(randomUUID(), versions, pageEnd),
      armed,
      recorded,
      commitLifecycleBatchResults(recorded),
    ]) {
      const encoded = JSON.parse(JSON.stringify(encodeLifecycleContinuation(continuation)))
      expect(JSON.stringify(encoded)).not.toContain('"key"')
      expect(decodeLifecycleContinuation(encoded)).toEqual(continuation)
    }
  })

  test('bounds the durable logical-version batch', () => {
    const value = validContinuation()
    value.batch.versions = Array.from({ length: 481 }, () => value.batch.versions[0])
    expect(() => decodeLifecycleContinuation(value)).toThrow('at most 480 entries')
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
            archivedAt: page.pageEnd.archivedAt,
            rawRowNumber: 1,
          },
        ],
      })
    ).toThrow('Lifecycle pages with eligible candidates must be staged before advancing')
  })

  test('stages a batch with distinct examined and eligible counts, rejects restaging, and drops pageEnd on advance', () => {
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
    const staged = stageLifecycleBatch(initial, { pageEnd, rawRowsExamined: 5, versions })

    expect(staged.counters).toMatchObject({ versionsExamined: 5, versionsEligible: 2 })
    expect(() => stageLifecycleBatch(staged, { pageEnd, rawRowsExamined: 5, versions })).toThrow(
      'already has a staged batch'
    )

    const armed = armLifecycleAttempt(staged, {
      attemptId: randomUUID(),
      startedAt: '2026-08-15T00:01:00.000Z',
    })
    const resolved = recordLifecycleArtifactOutcomes(
      armed,
      new Map(
        versions.flatMap((v) => v.artifacts.map((a) => [a.key, { outcome: 'DELETED' as const }]))
      )
    )
    const advanced = commitLifecycleBatchResults(resolved)
    expect(advanced).not.toHaveProperty('pageEnd')
    expect(advanced).not.toHaveProperty('batch')
    expect(advanced.cursor).toEqual(pageEnd)
  })

  test('skips a staged batch to its page end without counting it as completed', () => {
    const pageEnd = { name: 'logs/example.json', archivedAt: '2026-08-02T00:00:00.000Z' }
    const staged = stagedContinuation(
      randomUUID(),
      [{ id: randomUUID(), name: pageEnd.name, version: randomUUID(), isDeleteMarker: false }],
      pageEnd
    )
    const { batch: _batch, pageEnd: _pageEnd, ...rest } = staged

    expect(skipLifecycleBatch(staged)).toEqual({ ...rest, cursor: pageEnd })
  })

  test('keeps null and literal null-string identities separate through recovery', () => {
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
      batch: { keySeparator: '/', versions: [legacy, named] },
    })

    const armed = armLifecycleAttempt(staged, initial.batch.inFlight)
    const recorded = recordLifecycleArtifactOutcomes(
      armed,
      new Map(named.artifacts.map(({ key }) => [key, { outcome: 'ABSENT' as const }]))
    )
    const recovered = commitLifecycleBatchResults(recorded)
    expect(recovered.batch).toEqual({
      keySeparator: '/',
      versions: [legacy],
      inFlight: initial.batch.inFlight,
    })
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
      batch: { keySeparator: '/', versions: [marker] },
    })

    expect(() => commitLifecycleBatchResults(staged)).toThrow('must be armed before committing')
    expect(() => recordLifecycleArtifactOutcomes(staged, new Map())).toThrow(
      'must be armed before recording'
    )

    const armed = armLifecycleAttempt(staged, initial.batch.inFlight)
    const completed = commitLifecycleBatchResults(armed)
    expect(completed.cursor).toEqual(staged.pageEnd)
    expect(completed.batch).toBeUndefined()
    expect(completed.counters).toEqual({ ...staged.counters, batchesCompleted: 1 })
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

    expect(commitLifecycleBatchResults(armed).batch?.versions).toHaveLength(1)

    const partiallyResolved = recordLifecycleArtifactOutcomes(
      armed,
      new Map([[candidate.artifacts[1].key, { outcome: 'DELETED' }]])
    )
    expect(partiallyResolved.batch?.inFlight).toEqual(attempt)
    expect(commitLifecycleBatchResults(partiallyResolved).batch?.versions).toHaveLength(1)

    const resolved = recordLifecycleArtifactOutcomes(
      partiallyResolved,
      new Map([[candidate.artifacts[0].key, { outcome: 'ABSENT' }]])
    )
    expect(commitLifecycleBatchResults(resolved)).toMatchObject({
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

  test('retains a version whose data artifact is unresolved even if the info sibling was deleted', () => {
    const continuation = decodeLifecycleContinuation(validContinuation())
    const version = continuation.batch!.versions[0]
    const partial = recordLifecycleArtifactOutcomes(
      continuation,
      new Map([
        [version.artifacts[0].key, { outcome: 'UNRESOLVED' as const, error: 'retry me' }],
        [version.artifacts[1].key, { outcome: 'DELETED' as const }],
      ])
    )

    expect(commitLifecycleBatchResults(partial)).toMatchObject({
      batch: {
        inFlight: partial.batch!.inFlight,
        versions: [
          {
            name: version.name,
            version: version.version,
            artifacts: [
              expect.objectContaining({ outcome: 'UNRESOLVED' }),
              expect.objectContaining({ outcome: 'DELETED' }),
            ],
          },
        ],
      },
      counters: { batchesCompleted: 0 },
    })
  })

  test.each([
    ['data FAILED with info DELETED', 'FAILED', 'DELETED'],
    ['data DELETED with info FAILED', 'DELETED', 'FAILED'],
    ['data ABSENT with info FAILED', 'ABSENT', 'FAILED'],
  ] as const)('retains a failed artifact for retry: %s', (_label, data, info) => {
    const continuation = decodeLifecycleContinuation(validContinuation())
    const version = continuation.batch!.versions[0]
    const recorded = recordLifecycleArtifactOutcomes(
      continuation,
      new Map([
        [version.artifacts[0].key, { outcome: data, error: 'x' }],
        [version.artifacts[1].key, { outcome: info, error: 'x' }],
      ])
    )

    expect(commitLifecycleBatchResults(recorded)).toMatchObject({
      batch: { versions: [expect.objectContaining({ id: version.id })] },
      counters: { batchesCompleted: 0 },
    })
  })

  test('saturates the cumulative byte counter at the safe integer limit', () => {
    const input = validContinuation()
    input.counters.bytesDeleted = Number.MAX_SAFE_INTEGER - 1
    const continuation = decodeLifecycleContinuation(input)
    expect(
      recordLifecycleMetadataDeletions(continuation, {
        objectVersions: 1,
        deleteMarkers: 0,
        bytes: 10,
      }).counters.bytesDeleted
    ).toBe(Number.MAX_SAFE_INTEGER)
  })

  test('abandonPending drains FAILED and UNRESOLVED artifacts and advances the cursor', () => {
    const armed = decodeLifecycleContinuation(validContinuation())
    const [data, info] = armed.batch!.versions[0].artifacts
    const recorded = recordLifecycleArtifactOutcomes(
      armed,
      new Map([
        [data.key, { outcome: 'FAILED' as const, error: 'not deleted' }],
        [info.key, { outcome: 'UNRESOLVED' as const }],
      ])
    )
    const abandoned = commitLifecycleBatchResults(recorded, true)
    expect(abandoned.batch).toBeUndefined()
    expect(abandoned.cursor).toEqual(armed.pageEnd)
    expect(abandoned.counters.batchesCompleted).toBe(armed.counters.batchesCompleted + 1)
  })

  test('retains definitive all-failed versions for retry', () => {
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
    expect(commitLifecycleBatchResults(failed)).toMatchObject({
      batch: { versions: [expect.objectContaining({ id: armed.batch!.versions[0].id })] },
      counters: { batchesCompleted: 0 },
    })
  })
})
