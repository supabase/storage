import fs from 'node:fs'
import fsp from 'node:fs/promises'
import os from 'node:os'
import { finished, PassThrough, type Readable } from 'node:stream'
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

  // Temporary workaround for @tus/s3-store 2.0.9 (unchanged in 2.1.0). Same as upstream
  // write(), except the incomplete part is joined with concatStreams instead of MultiStream.
  // Upstream builds `new MultiStream([incompletePart, src])` only after the S3 calls above.
  // If src fails during those calls (e.g. the StreamLimiter rejects an oversized body),
  // MultiStream subscribes too late and waits on the dead stream forever: the splitter keeps
  // an open chunk FileHandle and a part permit, and once the request is gone GC closes the
  // handle, which Node 26 raises as a fatal ERR_INVALID_STATE uncaught exception.
  override async write(src: Readable, id: string, offset: number): Promise<number> {
    const metadata = await this.getMetadata(id)
    const parts = await this.retrieveParts(id)
    const partNumber = parts.length > 0 ? (parts[parts.length - 1].PartNumber ?? 0) : 0
    const incompletePart = await this.downloadIncompletePart(id)
    const requestedOffset = offset

    if (incompletePart) {
      // Once the file is on disk, the incomplete part is deleted from S3.
      try {
        await this.deleteIncompletePart(id)
      } catch (error) {
        fsp.rm(incompletePart.path).catch(() => {})
        throw error
      }
      offset = requestedOffset - incompletePart.size
      src = concatStreams(incompletePart.createReader({ cleanUpOnEnd: true }), src)
    }

    const bytesUploaded = await this.uploadParts(metadata, src, partNumber + 1, offset)
    // The incomplete part was already counted in the requested offset.
    const newOffset = requestedOffset + bytesUploaded - (incompletePart?.size ?? 0)

    if (metadata.file.size === newOffset) {
      const parts = await this.retrieveParts(id)
      await this.finishMultipartUpload(metadata, parts)
      await this.completeMetadata(metadata.file)
      await this.clearCache(id)
    }

    return newOffset
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

// Unlike MultiStream, finished() and pipeline() also report a tail that already errored or
// closed before they were attached, so a dead request stream fails the write instead of
// stalling it. Any error destroys the output and both inputs with it, as MultiStream does.
function concatStreams(head: Readable, tail: Readable): Readable {
  const out = new PassThrough()
  const fail = (error: Error) => {
    out.destroy(error)
    head.destroy(error)
    tail.destroy(error)
  }
  // Watch the tail from the start: if the request fails while the head is still streaming
  // (e.g. its chunk waits for a part permit), the write and upload lock are released now.
  finished(tail, (error) => error && fail(error))
  pipeline(head, out, { end: false })
    .then(() => pipeline(tail, out))
    .catch(fail)
  return out
}
