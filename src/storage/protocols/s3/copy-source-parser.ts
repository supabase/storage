import { ERRORS } from '@internal/errors'

export function parseCopySource(copySource: string): {
  bucketName: string
  objectKey: string
  sourceVersion?: string
} {
  const normalizedCopySource = copySource.startsWith('/') ? copySource.slice(1) : copySource
  const [encodedPath, ...queryParts] = normalizedCopySource.split('?')
  const queryParams = queryParts.join('?')

  let decodedPath = ''
  try {
    decodedPath = decodeURIComponent(encodedPath)
  } catch {
    throw ERRORS.InvalidParameter('CopySource')
  }

  const separatorIdx = decodedPath.indexOf('/')
  if (separatorIdx <= 0) {
    throw ERRORS.MissingParameter('CopySource')
  }

  const bucketName = decodedPath.slice(0, separatorIdx)
  const objectKey = decodedPath.slice(separatorIdx + 1)
  if (!objectKey) {
    throw ERRORS.MissingParameter('CopySource')
  }

  const searchParams = new URLSearchParams(queryParams)
  const sourceVersion = searchParams.get('versionId') || undefined

  if (searchParams.has('versionId') && !sourceVersion) {
    throw ERRORS.InvalidParameter('CopySource')
  }

  return {
    bucketName,
    objectKey,
    sourceVersion,
  }
}
