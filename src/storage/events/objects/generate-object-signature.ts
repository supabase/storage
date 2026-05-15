import { createHash } from 'node:crypto'
import { Readable } from 'node:stream'
import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload } from '@internal/queue'
import { ObjectResponse } from '@storage/backend'
import { Job, Queue as PgBossQueue, SendOptions, WorkOptions } from 'pg-boss'
import { getConfig } from '../../../config'
import { ObjectSignatureCursor } from '../../database'
import { Storage } from '../../storage'
import { BaseEvent } from '../base-event'

const { storageS3Bucket } = getConfig()

const DEFAULT_BACKFILL_BATCH_SIZE = 500
const MAX_BACKFILL_BATCH_SIZE = 1000

interface GenerateObjectSignaturesPayload extends BasePayload {
  bucketId?: string
  objectNames?: string[]
  force?: boolean
  cursor?: ObjectSignatureCursor
  batchSize?: number
}

interface GenerateObjectSignaturePayload extends BasePayload {
  bucketId: string
  objectName: string
  version?: string
}

export class GenerateObjectSignatures extends BaseEvent<GenerateObjectSignaturesPayload> {
  static queueName = 'object:signatures:generate'
  protected static allowSync = false

  static getWorkerOptions(): WorkOptions {
    return {}
  }

  static getQueueOptions(): PgBossQueue {
    return {
      name: this.queueName,
      policy: 'singleton',
    } as const
  }

  static getSendOptions(payload: GenerateObjectSignaturesPayload): SendOptions {
    return {
      singletonKey: payload.tenant.ref,
      priority: 5,
      retryLimit: 5,
      retryDelay: 5,
    }
  }

  static async handle(job: Job<GenerateObjectSignaturesPayload>) {
    let storage: Storage | undefined
    const batchSize = normalizeBatchSize(job.data.batchSize)

    try {
      storage = await this.createStorage(job.data)
      const objects = await storage.db.listObjectsForSignatureGeneration({
        bucketId: job.data.bucketId,
        objectNames: job.data.objectNames,
        force: Boolean(job.data.force),
        cursor: job.data.cursor,
        limit: batchSize,
      })

      if (objects.length > 0) {
        await GenerateObjectSignature.batchSend(
          objects.map(
            (object) =>
              new GenerateObjectSignature({
                tenant: job.data.tenant,
                bucketId: object.bucket_id,
                objectName: object.name,
                version: object.version ?? undefined,
                reqId: job.data.reqId,
                sbReqId: job.data.sbReqId,
              })
          )
        )
      }

      const shouldReschedule = objects.length >= batchSize

      if (shouldReschedule) {
        const last = objects[objects.length - 1]
        await GenerateObjectSignatures.send({
          tenant: job.data.tenant,
          bucketId: job.data.bucketId,
          objectNames: job.data.objectNames,
          force: job.data.force,
          cursor: { bucketId: last.bucket_id, objectName: last.name },
          reqId: job.data.reqId,
          sbReqId: job.data.sbReqId,
          batchSize,
        })
      }

      logSchema.event(
        logger,
        `[Admin]: GenerateObjectSignatures ${job.data.tenant.ref} processed ${objects.length} objects`,
        {
          jobId: job.id,
          type: 'event',
          event: 'GenerateObjectSignatures',
          payload: JSON.stringify(job.data),
          objectPath: job.data.tenant.ref,
          tenantId: job.data.tenant.ref,
          project: job.data.tenant.ref,
          reqId: job.data.reqId,
          sbReqId: job.data.sbReqId,
          metadata: JSON.stringify({
            batchSize,
            bucketId: job.data.bucketId ?? null,
            cursor: job.data.cursor ?? null,
            force: Boolean(job.data.force),
            objectNamesCount: job.data.objectNames?.length ?? 0,
            objectsCount: objects.length,
            rescheduled: shouldReschedule,
          }),
        }
      )
    } catch (error) {
      logSignatureGenerationError({
        error,
        eventName: 'GenerateObjectSignatures',
        target: job.data.tenant.ref,
        payload: job.data,
        metadata: {
          batchSize,
          bucketId: job.data.bucketId,
          cursor: job.data.cursor,
          force: job.data.force,
          objectNamesCount: job.data.objectNames?.length,
        },
      })
      throw error
    } finally {
      disposeStorage(storage, 'GenerateObjectSignatures', job.data)
    }
  }
}

export class GenerateObjectSignature extends BaseEvent<GenerateObjectSignaturePayload> {
  static queueName = 'object:signature:generate'
  protected static allowSync = false

  static getWorkerOptions(): WorkOptions {
    return {}
  }

  static getQueueOptions(): PgBossQueue {
    return {
      name: this.queueName,
      policy: 'singleton',
    } as const
  }

  static getSendOptions(payload: GenerateObjectSignaturePayload): SendOptions {
    return {
      singletonKey: createHash('sha256')
        .update(
          JSON.stringify([
            payload.tenant.ref,
            payload.bucketId,
            payload.objectName,
            payload.version ?? null,
          ])
        )
        .digest('hex'),
      expireInMinutes: 120,
      priority: 5,
      retryLimit: 5,
      retryDelay: 5,
    }
  }

  static async handle(job: Job<GenerateObjectSignaturePayload>) {
    let storage: Storage | undefined
    let objectPath = `${job.data.tenant.ref}/${job.data.bucketId}/${job.data.objectName}`

    try {
      storage = await this.createStorage(job.data)
      objectPath = storage.location.getKeyLocation({
        tenantId: job.data.tenant.ref,
        bucketId: job.data.bucketId,
        objectName: job.data.objectName,
      })

      const response = await storage.backend.getObject(
        storageS3Bucket,
        objectPath,
        job.data.version
      )
      const sha256 = await digestObjectBody(response.body)
      await storage.db.updateObjectSignature(
        job.data.bucketId,
        job.data.objectName,
        job.data.version,
        Buffer.from(sha256, 'hex')
      )

      logSchema.event(logger, `[Admin]: GenerateObjectSignature ${objectPath}`, {
        jobId: job.id,
        type: 'event',
        event: 'GenerateObjectSignature',
        payload: JSON.stringify(job.data),
        objectPath,
        resources: [`${job.data.bucketId}/${job.data.objectName}`],
        tenantId: job.data.tenant.ref,
        project: job.data.tenant.ref,
        reqId: job.data.reqId,
        sbReqId: job.data.sbReqId,
        metadata: JSON.stringify({ version: job.data.version ?? null }),
      })
    } catch (error) {
      logSignatureGenerationError({
        error,
        eventName: 'GenerateObjectSignature',
        target: objectPath,
        payload: job.data,
        metadata: {
          bucketId: job.data.bucketId,
          objectName: job.data.objectName,
          objectPath,
          version: job.data.version,
        },
      })
      throw error
    } finally {
      disposeStorage(storage, 'GenerateObjectSignature', job.data)
    }
  }
}

function normalizeBatchSize(batchSize: number | undefined) {
  if (!batchSize || !Number.isFinite(batchSize) || batchSize < 1) {
    return DEFAULT_BACKFILL_BATCH_SIZE
  }

  return Math.min(Math.floor(batchSize), MAX_BACKFILL_BATCH_SIZE)
}

async function digestObjectBody(body: ObjectResponse['body']) {
  if (!body) {
    throw ERRORS.InternalError(undefined, 'Object body is missing for SHA-256 hashing')
  }

  const hash = createHash('sha256')

  if (Buffer.isBuffer(body)) {
    hash.update(body)
    return hash.digest('hex')
  }

  if (body instanceof Blob) {
    await updateHashFromReadableStream(hash, body.stream())
    return hash.digest('hex')
  }

  if (body instanceof Readable) {
    await updateHashFromIterable(hash, body)
    return hash.digest('hex')
  }

  if (isReadableStream(body)) {
    await updateHashFromReadableStream(hash, body)
    return hash.digest('hex')
  }

  throw ERRORS.InternalError(undefined, 'Unsupported object body type for SHA-256 hashing')
}

async function updateHashFromIterable(
  hash: ReturnType<typeof createHash>,
  body: AsyncIterable<unknown>
) {
  for await (const chunk of body) {
    if (typeof chunk === 'string') {
      throw ERRORS.InternalError(
        undefined,
        'Unsupported object body string chunk for SHA-256 hashing'
      )
    }

    if (Buffer.isBuffer(chunk)) {
      hash.update(chunk)
      continue
    }

    if (chunk instanceof Uint8Array) {
      hash.update(chunk)
      continue
    }

    if (chunk instanceof ArrayBuffer) {
      hash.update(new Uint8Array(chunk))
      continue
    }

    throw ERRORS.InternalError(undefined, 'Unsupported object body chunk for SHA-256 hashing')
  }
}

async function updateHashFromReadableStream(
  hash: ReturnType<typeof createHash>,
  body: ReadableStream<Uint8Array>
) {
  const reader = body.getReader()

  try {
    let result = await reader.read()
    while (!result.done) {
      if (typeof result.value === 'string') {
        throw ERRORS.InternalError(
          undefined,
          'Unsupported object body string chunk for SHA-256 hashing'
        )
      }

      hash.update(result.value)
      result = await reader.read()
    }
  } finally {
    reader.releaseLock()
  }
}

function isReadableStream(body: unknown): body is ReadableStream<Uint8Array> {
  return Boolean(
    body && typeof body === 'object' && typeof (body as ReadableStream).getReader === 'function'
  )
}

function disposeStorage(storage: Storage | undefined, eventName: string, payload: BasePayload) {
  storage?.db.destroyConnection().catch((error) => {
    logSignatureGenerationError({
      error,
      eventName,
      target: payload.tenant.ref,
      payload,
      suffix: 'FAILED DISPOSING CONNECTION',
    })
  })
}

function logSignatureGenerationError({
  error,
  eventName,
  target,
  payload,
  metadata,
  suffix = 'FAILED',
}: {
  error: unknown
  eventName: string
  target: string
  payload: BasePayload
  metadata?: Record<string, unknown>
  suffix?: string
}) {
  logSchema.error(logger, `[Admin]: ${eventName} ${target} - ${suffix}`, {
    error,
    type: 'event',
    event: eventName,
    ...(metadata ? { metadata: JSON.stringify(metadata) } : {}),
    tenantId: payload.tenant.ref,
    project: payload.tenant.ref,
    reqId: payload.reqId,
    sbReqId: payload.sbReqId,
  })
}
