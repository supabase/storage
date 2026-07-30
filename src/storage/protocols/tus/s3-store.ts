import type fs from 'node:fs'
import type stream from 'node:stream'
import type { Part } from '@aws-sdk/client-s3'
import { type MetadataValue, type Options, S3Store as TusS3Store } from '@tus/s3-store'
import { ERRORS as TUS_ERRORS } from '@tus/utils'
import { markTusWriteMutation, writeWithRequestCompletion } from './request-context'

const MISSING_RESOURCE_CODES = new Set(['NotFound', 'NoSuchKey', 'NoSuchUpload'])

function isMissingResourceError(error: unknown): boolean {
  if (!error || typeof error !== 'object') {
    return false
  }

  if (error === TUS_ERRORS.FILE_NOT_FOUND) {
    return true
  }

  const candidate = error as Record<string, unknown>
  const name = candidate.name // canonical
  if (typeof name === 'string' && MISSING_RESOURCE_CODES.has(name)) {
    return true
  }

  const upperCode = candidate.Code // compat, upstream s3-store checks this as well
  if (typeof upperCode === 'string' && MISSING_RESOURCE_CODES.has(upperCode)) {
    return true
  }

  const lowerCode = candidate.code // defensive check
  return typeof lowerCode === 'string' && MISSING_RESOURCE_CODES.has(lowerCode)
}

export class S3Store extends TusS3Store {
  constructor(options: Options) {
    super(options)
    this.client.middlewareStack.remove('loggerMiddleware')
  }

  write(readable: stream.Readable, id: string, offset: number): Promise<number> {
    return writeWithRequestCompletion(
      this,
      's3',
      id,
      offset,
      () => super.write(readable, id, offset),
      { trackedMutations: true }
    )
  }

  protected uploadPart(
    metadata: MetadataValue,
    readStream: fs.ReadStream | stream.Readable | Buffer,
    partNumber: number
  ): Promise<string> {
    markTusWriteMutation()
    return super.uploadPart(metadata, readStream, partNumber)
  }

  protected uploadIncompletePart(
    id: string,
    readStream: fs.ReadStream | stream.Readable
  ): Promise<string> {
    markTusWriteMutation()
    return super.uploadIncompletePart(id, readStream)
  }

  protected deleteIncompletePart(id: string): Promise<void> {
    markTusWriteMutation()
    return super.deleteIncompletePart(id)
  }

  protected finishMultipartUpload(
    metadata: MetadataValue,
    parts: Part[]
  ): Promise<string | undefined> {
    markTusWriteMutation()
    return super.finishMultipartUpload(metadata, parts)
  }

  // Owns a checked removal sequence because upstream does not clean completed
  // multipart uploads or .part and ignores per-key DeleteObjects errors. Abort
  // settles before data deletion so a concurrent multipart completion cannot
  // recreate the object after deletion. Its failure does not short-circuit the
  // remaining cleanup, and .info remains unless every phase succeeds.
  async remove(id: string): Promise<void> {
    let multipartUploadId: string | undefined
    try {
      const metadata = await this.getMetadata(id)
      multipartUploadId = metadata['upload-id']
    } catch (error) {
      if (isMissingResourceError(error)) {
        throw TUS_ERRORS.FILE_NOT_FOUND
      }
      throw error
    }

    await this.deleteUploadArtifacts(id, multipartUploadId)
  }

  private async deleteUploadArtifacts(
    id: string,
    multipartUploadId: string | undefined
  ): Promise<void> {
    const [abortResult] = await Promise.allSettled([
      this.abortMultipartUpload(id, multipartUploadId),
    ])
    const cleanupResults = await Promise.allSettled([
      this.deleteObjectsWithRetry(id, [id, this.partKey(id, true)]),
      this.clearCache(id),
    ])
    const results = [abortResult, ...cleanupResults]
    const errors = results.flatMap((result) =>
      result.status === 'rejected' ? [result.reason] : []
    )

    if (errors.length === 1) {
      throw errors[0]
    }

    if (errors.length > 1) {
      throw new AggregateError(errors, `Failed to remove TUS upload ${id}`)
    }

    // .info is the durable discovery record for direct cleanup. Delete it only
    // after every data-bearing object and live cache entry is gone.
    await this.deleteObjectsWithRetry(id, [this.infoKey(id)])
  }

  private async abortMultipartUpload(
    id: string,
    multipartUploadId: string | undefined
  ): Promise<void> {
    if (!multipartUploadId) {
      return
    }

    try {
      await this.client.abortMultipartUpload({
        Bucket: this.bucket,
        Key: id,
        UploadId: multipartUploadId,
      })
    } catch (error) {
      // A completed multipart upload reports a missing upload id on abort;
      // its object, .info, .part, and cache entries still need deletion.
      if (!isMissingResourceError(error)) {
        throw error
      }
    }
  }

  private async deleteObjectsWithRetry(id: string, keys: string[]): Promise<void> {
    const allObjects = keys.map((Key) => ({ Key }))
    let objects = allObjects
    let errors: Error[] = []

    // Retry only the reported failures without repeating confirmed deletions.
    for (let attempt = 0; attempt < 2; attempt++) {
      const output = await this.client.deleteObjects({
        Bucket: this.bucket,
        Delete: { Objects: objects },
      })
      const outputErrors = output?.Errors ?? []
      errors = outputErrors.map((error) => {
        const code = error.Code ?? 'UnknownError'
        const key = error.Key ?? 'unknown key'
        const message = error.Message ? `: ${error.Message}` : ''
        return Object.assign(new Error(`${code} deleting ${key}${message}`), {
          code,
          key,
        })
      })

      if (errors.length === 0) {
        return
      }

      const failedKeys = outputErrors.flatMap((error) =>
        error.Key === undefined ? [] : [error.Key]
      )
      objects =
        failedKeys.length === outputErrors.length
          ? Array.from(new Set(failedKeys), (Key) => ({ Key }))
          : allObjects
    }

    throw new AggregateError(errors, `Failed to delete TUS upload artifacts for ${id}`)
  }
}
