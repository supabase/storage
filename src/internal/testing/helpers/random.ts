import { randomBytes, randomUUID } from 'node:crypto'

/**
 * Each test *file* is assigned a short prefix at import time (from
 * context.ts). We keep prefixes short so they fit inside bucket name
 * constraints (storage.buckets.name is validated by mustBeValidBucketName — no
 * leading/trailing whitespace, reasonable length).
 *
 * Format: `v2{6 hex chars}` → e.g. `v2a7f3b1`
 */
export function makeFilePrefix(): string {
  return `v2${randomBytes(3).toString('hex')}`
}

export function uniqueName(prefix: string, label = ''): string {
  const suffix = randomBytes(3).toString('hex')
  return label ? `${prefix}_${label}_${suffix}` : `${prefix}_${suffix}`
}

export { randomUUID }
