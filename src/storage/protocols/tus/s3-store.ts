import fs from 'node:fs'
import fsp from 'node:fs/promises'
import os from 'node:os'
import type { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { logger } from '@internal/monitoring'
import { type MetadataValue, type Options, S3Store as TusS3Store } from '@tus/s3-store'
import { TUS_RESUMABLE, type Upload } from '@tus/server'
import { PartUploadSemaphore } from './part-upload-semaphore'
import { TusChunkSplitter } from './splitter-workaround'

export class S3Store extends TusS3Store {
  protected readonly chunkSemaphore: PartUploadSemaphore

  constructor(options: Options) {
    super(options)
    this.chunkSemaphore = new PartUploadSemaphore(options.maxConcurrentPartUploads ?? 60)
    this.client.middlewareStack.remove('loggerMiddleware')
  }

  // Temporary workaround for @tus/s3-store 2.0.9 and @tus/utils 0.7.2.
  // Remove this override, TusChunkSplitter, and PartUploadSemaphore after
  // upgrading to compatible upstream versions that fix interrupted uploads.
  protected override async uploadParts(
    metadata: MetadataValue,
    readStream: Readable,
    currentPartNumber: number,
    offset: number
  ): Promise<number> {
    const size = metadata.file.size
    const uploads: Promise<void>[] = []
    let pendingChunkPath: string | null = null
    let bytesUploaded = 0
    let permit: Awaited<ReturnType<PartUploadSemaphore['acquire']>> | undefined
    const stopped = new AbortController()
    const splitter = new TusChunkSplitter({
      chunkSize: this.calcOptimalPartSize(size),
      directory: os.tmpdir(),
    })
      .on('close', () => stopped.abort(new Error('TUS chunk splitter was destroyed')))
      .on('beforeChunkStarted', async () => {
        permit = await this.chunkSemaphore.acquire(stopped.signal)
      })
      .on('chunkStarted', (filepath) => {
        pendingChunkPath = filepath
      })
      .on('chunkFinished', ({ path, size: partSize }) => {
        pendingChunkPath = null
        const acquiredPermit = permit
        permit = undefined
        const partNumber = currentPartNumber++
        offset += partSize
        const isFinalPart = size === offset

        // biome-ignore lint/suspicious/noAsyncPromiseExecutor: matches upstream uploadParts
        const upload = new Promise<void>(async (resolve, reject) => {
          let readable: fs.ReadStream | undefined
          try {
            readable = fs.createReadStream(path)
            readable.on('error', reject)
            if (partSize >= this.minPartSize || isFinalPart) {
              await this.uploadPart(metadata, readable, partNumber)
            } else {
              await this.uploadIncompletePart(metadata.file.id, readable)
            }
            bytesUploaded += partSize
            resolve()
          } catch (error) {
            splitter.destroy(error as Error)
            reject(error)
          } finally {
            readable?.destroy()
            fsp.rm(path).catch(() => {})
            acquiredPermit?.release().catch(() => {})
          }
        })
        // The upload may reject before pipeline settles and the array is awaited.
        upload.catch(() => {})
        uploads.push(upload)
      })
      .on('chunkError', () => {
        permit?.release().catch(() => {})
      })

    try {
      await pipeline(readStream, splitter)
    } catch (error) {
      if (pendingChunkPath !== null) {
        await fsp.rm(pendingChunkPath, { force: true }).catch((error) => {
          logger.debug({ err: error, path: pendingChunkPath }, 'Failed to remove TUS chunk')
        })
      }
      uploads.push(Promise.reject(error))
    } finally {
      await Promise.allSettled(uploads)
      await Promise.all(uploads)
    }

    return bytesUploaded
  }

  async create(upload: Upload): Promise<Upload> {
    if (!upload.metadata?.contentEncoding) {
      return super.create(upload)
    }

    // Upstream's create() forwards ContentType and CacheControl but omits ContentEncoding.
    upload.creation_date = new Date().toISOString()

    const result = await this.client.createMultipartUpload({
      Bucket: this.bucket,
      Key: upload.id,
      Metadata: { 'tus-version': TUS_RESUMABLE },
      ContentType: upload.metadata.contentType || undefined,
      CacheControl: upload.metadata.cacheControl || undefined,
      ContentEncoding: upload.metadata.contentEncoding,
    })

    upload.storage = { type: 's3', path: result.Key as string, bucket: this.bucket }
    await this.saveMetadata(upload, result.UploadId as string)
    return upload
  }
}
