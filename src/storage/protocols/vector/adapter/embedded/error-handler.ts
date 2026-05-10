import { ERRORS } from '@internal/errors'

export type ZVecError = Error & {
  code:
    | 'ZVEC_NOT_FOUND'
    | 'ZVEC_ALREADY_EXISTS'
    | 'ZVEC_INVALID_ARGUMENT'
    | 'ZVEC_PERMISSION_DENIED'
    | 'ZVEC_FAILED_PRECONDITION'
    | 'ZVEC_RESOURCE_EXHAUSTED'
    | 'ZVEC_UNAVAILABLE'
    | 'ZVEC_INTERNAL_ERROR'
    | 'ZVEC_NOT_SUPPORTED'
    | 'ZVEC_UNKNOWN'
    | 'ZVEC_INVALID_STATUS_CODE'
}

function hasZVecCode(error: unknown): error is ZVecError {
  return (
    error instanceof Error &&
    typeof (error as ZVecError).code === 'string' &&
    (error as ZVecError).code.startsWith('ZVEC_')
  )
}

export async function handleZVecError<T>(
  fn: () => Promise<T> | T,
  resource: { type: string; name: string }
): Promise<T> {
  try {
    return await fn()
  } catch (e) {
    if (!hasZVecCode(e)) {
      throw e
    }

    switch (e.code) {
      case 'ZVEC_NOT_FOUND':
        throw ERRORS.S3VectorNotFoundException(resource.type, resource.name)
      case 'ZVEC_ALREADY_EXISTS':
        throw ERRORS.S3VectorConflictException(resource.type, resource.name)
      case 'ZVEC_INVALID_ARGUMENT':
        throw ERRORS.InvalidParameter(`${resource.type}:${resource.name}`, { message: e.message })
      case 'ZVEC_PERMISSION_DENIED':
        throw ERRORS.AccessDenied(e.message, e)
      case 'ZVEC_NOT_SUPPORTED':
        throw ERRORS.S3VectorEmbeddedNotSupported(`${resource.type}:${resource.name}`)
      default:
        throw e
    }
  }
}
