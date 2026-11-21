import { LIFECYCLE_MAX_DELETE_BATCH_SIZE, type LifecycleEvaluationPage } from '../schemas/lifecycle'

export function validateLifecycleDeleteBatchSize(size: number): void {
  if (!Number.isSafeInteger(size) || size < 1 || size > LIFECYCLE_MAX_DELETE_BATCH_SIZE) {
    throw new Error(
      `Lifecycle delete batch size must be between 1 and ${LIFECYCLE_MAX_DELETE_BATCH_SIZE}`
    )
  }
}

export function* lifecycleEvaluationBatches(
  page: LifecycleEvaluationPage,
  size: number
): Generator<Omit<LifecycleEvaluationPage, 'exhausted'>> {
  validateLifecycleDeleteBatchSize(size)
  let consumed = 0
  for (let offset = 0; offset < page.candidates.length; offset += size) {
    const candidates = page.candidates.slice(offset, offset + size)
    const last = candidates[candidates.length - 1]
    const final = offset + size >= page.candidates.length
    const end = final ? page.rawRowsExamined : last.rawRowNumber
    yield {
      candidates,
      rawRowsExamined: end - consumed,
      pageEnd: final ? page.pageEnd : { name: last.name, archivedAt: last.archivedAt },
    }
    consumed = end
  }
}
