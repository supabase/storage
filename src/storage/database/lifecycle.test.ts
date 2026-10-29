import { compileLifecycleEvaluationRules } from '../lifecycle/configuration'
import {
  buildEvaluateNoncurrentLifecyclePageStatement,
  decodeLifecycleCandidateIdentity,
  mapLifecycleEvaluationPage,
} from './lifecycle'

const cursor = { name: 'last', archivedAt: '2026-08-01T00:00:00.000Z' }

describe('noncurrent lifecycle evaluation query', () => {
  test.each([
    {
      label: 'BC timestamp near the PostgreSQL minimum',
      timestamp: '-004713-11-26T00:00:00.000123Z',
      expected: '4714-11-26T00:00:00.000123Z BC',
    },
    {
      label: 'astronomical year zero',
      timestamp: '0000-01-01T00:00:00.123456Z',
      expected: '0001-01-01T00:00:00.123456Z BC',
    },
    {
      label: 'expanded astronomical year zero',
      timestamp: '+000000-01-01T00:00:00.123456Z',
      expected: '0001-01-01T00:00:00.123456Z BC',
    },
    {
      label: 'BC offset crossing into year zero',
      timestamp: '-000001-12-31T23:59:59.123456-08:00',
      expected: '0001-01-01T07:59:59.123456Z BC',
    },
    {
      label: 'AD offset crossing into year zero',
      timestamp: '0001-01-01T00:00:00.000123+01:00',
      expected: '0001-12-31T23:00:00.000123Z BC',
    },
  ])('binds $label without losing fractional seconds', ({ timestamp, expected }) => {
    const statement = buildEvaluateNoncurrentLifecyclePageStatement({
      bucketId: 'bucket-a',
      snapshotAt: timestamp,
      cursor: { name: 'logs/a', archivedAt: timestamp },
      pageSize: 1,
      rules: [{ cutoffAt: timestamp }],
    })

    expect(statement.values).toEqual([
      'bucket-a',
      expected,
      expected,
      expected,
      'logs/a',
      1,
      [expected],
      [null],
      1,
    ])
  })

  test('binds an empty cutoff at the PostgreSQL timestamp minimum', () => {
    const snapshotAt = new Date('2026-08-06T00:00:00.000Z')
    const noncurrentDays =
      (snapshotAt.getTime() - Date.parse('-004713-11-24T00:00:00.000Z')) / 86_400_000
    const rules = compileLifecycleEvaluationRules(
      {
        rules: [
          {
            status: 'Enabled',
            filter: {},
            noncurrentVersionExpiration: { noncurrentDays },
          },
        ],
      },
      snapshotAt
    )
    const statement = buildEvaluateNoncurrentLifecyclePageStatement({
      bucketId: 'bucket-a',
      snapshotAt: snapshotAt.toISOString(),
      pageSize: 1,
      rules,
    })

    expect(statement.values?.[2]).toBe('-infinity')
    expect(statement.values?.[4]).toEqual(['-infinity'])
  })

  test('maps page progress independently from candidate selectivity', () => {
    expect(
      mapLifecycleEvaluationPage(
        {
          raw_rows_examined: 2,
          exhausted: true,
          candidates: [],
          page_end: {
            name: 'last',
            archivedAt: '2026-08-01T00:00:00.000Z',
          },
        },
        480
      )
    ).toEqual({
      rawRowsExamined: 2,
      candidates: [],
      pageEnd: {
        name: 'last',
        archivedAt: '2026-08-01T00:00:00.000Z',
      },
      exhausted: true,
    })
  })

  test.each([
    [
      'more raw rows than the page size',
      { raw_rows_examined: 3, candidates: [], page_end: cursor },
    ],
    [
      'more candidates than raw rows',
      {
        raw_rows_examined: 0,
        candidates: [{ id: 'a', name: 'n', version: null, isDeleteMarker: false }],
        page_end: null,
      },
    ],
    ['rows without a page end', { raw_rows_examined: 1, candidates: [], page_end: null }],
    ['a page end without rows', { raw_rows_examined: 0, candidates: [], page_end: cursor }],
  ])('rejects %s', (_label, row) => {
    expect(() => mapLifecycleEvaluationPage({ ...row, exhausted: true }, 2)).toThrow()
  })

  test.each([null, 'version-a'])(
    'maps the physical candidate identity with version %s',
    (version) => {
      const id = '5a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d'
      expect(
        mapLifecycleEvaluationPage(
          {
            raw_rows_examined: 1,
            exhausted: true,
            candidates: [
              {
                id,
                name: 'history/object.txt',
                version,
                isDeleteMarker: false,
                archivedAt: '2026-08-01T00:00:00.000123Z',
                rawRowNumber: 1,
              },
            ],
            page_end: {
              name: 'history/object.txt',
              archivedAt: '2026-08-01T00:00:00.000Z',
            },
          },
          480
        )
      ).toMatchObject({
        candidates: [{ id, name: 'history/object.txt', version, isDeleteMarker: false }],
      })
    }
  )

  test.each([0, 481])('rejects page size %s', (pageSize) => {
    expect(() =>
      buildEvaluateNoncurrentLifecyclePageStatement({
        bucketId: 'bucket-a',
        snapshotAt: '2026-08-15T00:00:00.000Z',
        pageSize,
        rules: [{ cutoffAt: '2026-08-01T00:00:00.000Z' }],
      })
    ).toThrow('between 1 and 480')
  })

  test.each([0, -1, 1.5, 3, Number.NaN, '1', undefined])(
    'rejects invalid candidate position %s',
    (rawRowNumber) => {
      expect(() =>
        mapLifecycleEvaluationPage(
          {
            raw_rows_examined: 2,
            exhausted: true,
            page_end: cursor,
            candidates: [
              {
                id: 'a',
                name: 'a',
                version: null,
                isDeleteMarker: false,
                archivedAt: cursor.archivedAt,
                rawRowNumber,
              },
            ],
          },
          100
        )
      ).toThrow('invalid candidate row number')
    }
  )

  test.each([
    [1, 1],
    [2, 1],
  ])('rejects unordered candidate positions %j', (...positions) => {
    expect(() =>
      mapLifecycleEvaluationPage(
        {
          raw_rows_examined: 2,
          exhausted: true,
          page_end: cursor,
          candidates: positions.map((rawRowNumber) => ({
            id: 'a',
            name: 'a',
            version: null,
            isDeleteMarker: false,
            archivedAt: cursor.archivedAt,
            rawRowNumber,
          })),
        },
        100
      )
    ).toThrow('invalid candidate row number')
  })
})

describe('lifecycle candidate decoding', () => {
  const row = {
    id: '5a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d',
    name: 'history/object.txt',
    version: null,
    isDeleteMarker: false,
  }

  test.each([undefined, '', 1])(
    'rejects an omitted or malformed physical version %s',
    (version) => {
      expect(() => decodeLifecycleCandidateIdentity({ ...row, version })).toThrow(
        'candidate.version'
      )
    }
  )

  test.each([undefined, '', 42])('rejects an omitted or malformed row id %s', (id) => {
    expect(() => decodeLifecycleCandidateIdentity({ ...row, id })).toThrow('candidate.id')
  })
})
