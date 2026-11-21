import type { LifecycleEvaluationPage } from '../schemas/lifecycle'
import { lifecycleEvaluationBatches, validateLifecycleDeleteBatchSize } from './evaluation'

test.each([0, -1, 1.5, 49, Number.NaN, Number.POSITIVE_INFINITY])(
  'rejects delete batch size %s',
  (size) => {
    expect(() => validateLifecycleDeleteBatchSize(size)).toThrow('between 1 and 48')
  }
)

const cases = [
  { length: 0, size: 48, selectivity: 'none' },
  { length: 1, size: 1, selectivity: 'all' },
  { length: 48, size: 48, selectivity: 'all' },
  { length: 49, size: 48, selectivity: 'all' },
  { length: 96, size: 48, selectivity: 'all' },
  { length: 100, size: 48, selectivity: 'all' },
  { length: 480, size: 48, selectivity: 'all' },
  { length: 100, size: 1, selectivity: 'sparse' },
  { length: 100, size: 2, selectivity: 'sparse' },
  { length: 100, size: 17, selectivity: 'sparse' },
  { length: 480, size: 48, selectivity: 'sparse' },
  { length: 480, size: 48, selectivity: 'none' },
]

test.each(cases)(
  'partitions $length raw rows, $selectivity eligibility, limit $size without gaps',
  ({ length, size, selectivity }) => {
    const rows = Array.from({ length }, (_, index) => ({
      id: String(index),
      name: `name-${index}`,
      version: String(index),
      isDeleteMarker: index % 2 === 0,
      archivedAt: `2026-08-01T00:00:00.${String(index).padStart(6, '0')}Z`,
      rawRowNumber: index + 1,
    }))
    const candidates = rows.filter(
      (_, index) => selectivity === 'all' || (selectivity === 'sparse' && index % 3 === 1)
    )
    const last = rows.at(-1)
    const page: LifecycleEvaluationPage = {
      rawRowsExamined: length,
      candidates,
      ...(last ? { pageEnd: { name: last.name, archivedAt: last.archivedAt } } : {}),
      exhausted: length < 480,
    }
    const batches = [...lifecycleEvaluationBatches(page, size)]
    expect(batches.flatMap((batch) => batch.candidates)).toEqual(candidates)
    if (!candidates.length) {
      expect(batches).toEqual([])
      return
    }
    expect(batches.reduce((sum, batch) => sum + batch.rawRowsExamined, 0)).toBe(length)
    expect(batches.at(-1)?.pageEnd).toEqual(page.pageEnd)
    let consumed = 0
    for (const batch of batches) {
      expect(batch.candidates.length).toBeGreaterThan(0)
      expect(batch.candidates.length).toBeLessThanOrEqual(size)
      expect(batch.rawRowsExamined).toBeGreaterThanOrEqual(batch.candidates.length)
      consumed += batch.rawRowsExamined
      const boundary = rows[consumed - 1]
      expect(batch.pageEnd).toEqual({ name: boundary.name, archivedAt: boundary.archivedAt })
      // A restart at every boundary must see every remaining candidate exactly once.
      const remainder = candidates.filter((candidate) => candidate.rawRowNumber > consumed)
      const processed = batches
        .slice(0, batches.indexOf(batch) + 1)
        .flatMap((part) => part.candidates)
      expect([...processed, ...remainder]).toEqual(candidates)
    }
  }
)
