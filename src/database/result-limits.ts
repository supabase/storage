import { Buffer } from 'node:buffer'
import type { DatabaseConfig } from './config.js'
import { DatabaseWattError } from './errors.js'
import type { QueryResponse } from './types.js'

export function enforceResultLimits<T>(
  response: QueryResponse<T>,
  config: DatabaseConfig
): QueryResponse<T> {
  if (response.rows.length > config.maxResultRows) {
    throw new DatabaseWattError('RESULT_TOO_LARGE', 'Result row limit exceeded')
  }

  const serializedSize = Buffer.byteLength(JSON.stringify(response))
  if (serializedSize > config.maxResultBytes) {
    throw new DatabaseWattError('RESULT_TOO_LARGE', 'Result byte limit exceeded')
  }

  return response
}
