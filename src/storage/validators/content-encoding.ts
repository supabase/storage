import { ERRORS } from '@internal/errors'
import { isValidHeaderValue } from '@internal/http/header'

const CONTENT_CODINGS =
  /^[ \t]*(?:[!#$%&'*+.^_`|~0-9A-Za-z-]+[ \t]*)?(?:,[ \t]*(?:[!#$%&'*+.^_`|~0-9A-Za-z-]+[ \t]*)?)*$/

export function validateContentEncoding(value: unknown): string | undefined {
  if (value === undefined || value === '') {
    return undefined
  }

  if (typeof value !== 'string' || !isValidHeaderValue(value) || !CONTENT_CODINGS.test(value)) {
    throw ERRORS.InvalidParameter('contentEncoding')
  }

  return normalizeContentEncoding(value)
}

export function normalizeContentEncoding(value: string | undefined): string | undefined {
  // Neither transport framing nor identity describes a stored content coding.
  return (
    value
      ?.split(',')
      .map((coding) => coding.trim())
      .filter((coding) => coding !== '' && !/^(aws-chunked|identity)$/i.test(coding))
      .join(', ') || undefined
  )
}
