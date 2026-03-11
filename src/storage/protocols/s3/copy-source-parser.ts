import { ERRORS } from '@internal/errors'

const VERSION_ID_QUERY_DELIMITER = '?versionId='

function splitCopySourceVersion(copySource: string): {
  encodedPath: string
  sourceVersion?: string
} {
  const versionQueryIdx = copySource.lastIndexOf(VERSION_ID_QUERY_DELIMITER)

  if (versionQueryIdx === -1) {
    return {
      encodedPath: copySource,
    }
  }

  const sourceVersion = copySource.slice(versionQueryIdx + VERSION_ID_QUERY_DELIMITER.length)
  if (!sourceVersion) {
    throw ERRORS.InvalidParameter('CopySource')
  }

  return {
    encodedPath: copySource.slice(0, versionQueryIdx),
    sourceVersion,
  }
}

export function parseCopySource(copySource: string): {
  bucketName: string
  objectKey: string
  sourceVersion?: string
} {
  const normalizedCopySource = copySource.startsWith('/') ? copySource.slice(1) : copySource
  // Preserve raw '?' characters in partially encoded keys and only peel off a trailing versionId suffix.
  const { encodedPath, sourceVersion } = splitCopySourceVersion(normalizedCopySource)

  let decodedPath = ''
  try {
    decodedPath = decodeURIComponent(encodedPath)
  } catch {
    throw ERRORS.InvalidParameter('CopySource')
  }

  if (decodedPath.startsWith('/')) {
    decodedPath = decodedPath.slice(1)
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

  return {
    bucketName,
    objectKey,
    sourceVersion,
  }
}
