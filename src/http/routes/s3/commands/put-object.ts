import { MultipartFields } from '@fastify/multipart'
import { ERRORS } from '@internal/errors'
import { ByteCounterStream } from '@internal/streams'
import { MAX_PART_SIZE, S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { fileUploadFromRequest, getStandardMaxFileSizeLimit } from '@storage/uploader'
import stream, { Readable, Transform } from 'stream'
import { pipeline } from 'stream/promises'
import { ROUTE_OPERATIONS } from '../../operations'
import { S3Router } from '../router'

const PutObjectInput = {
  summary: 'Put Object',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
      '*': { type: 'string' },
    },
    required: ['Bucket', '*'],
  },
  Querystring: {
    type: 'object',
  },
  Headers: {
    type: 'object',
    properties: {
      authorization: { type: 'string' },
      host: { type: 'string' },
      'x-amz-content-sha256': { type: 'string' },
      'x-amz-decoded-content-length': { type: 'integer', minimum: 0, finite: true },
      'x-amz-date': { type: 'string' },
      'content-type': { type: 'string' },
      'content-length': { type: 'integer', minimum: 0, finite: true },
      'cache-control': { type: 'string' },
      'content-disposition': { type: 'string' },
      'content-encoding': { type: 'string' },
      expires: { type: 'string' },
    },
  },
} as const

const PostFormInput = {
  summary: 'PostForm Object',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
    },
    required: ['Bucket'],
  },
} as const

type PipelineBody = NodeJS.ReadableStream
type PipelineHandlerInput = AsyncIterable<unknown>

function withReadableStreamHandler<T>(
  handler: (fileStream: Readable, signal: AbortSignal | undefined) => Promise<T>
) {
  return async (fileStream: PipelineHandlerInput, options?: { signal?: AbortSignal }) => {
    // stream/promises exposes the final stream to the destination callback
    // as a generic async iterable. In these handlers the upstream is always
    // a Node readable, so narrow once here.
    return handler(fileStream as Readable, options?.signal)
  }
}

function pipelineWithOptionalStreamingSignature<T>(
  body: PipelineBody,
  limit: number,
  streamingSignatureV4: Transform | undefined,
  declaredContentLength: number | undefined,
  handler: (fileStream: Readable, signal: AbortSignal | undefined) => Promise<T>,
  signal: AbortSignal
) {
  if (streamingSignatureV4) {
    if (declaredContentLength === undefined) throw ERRORS.MissingContentLength()
    if (declaredContentLength > limit) throw ERRORS.EntityTooLarge()
    return pipeline(
      body,
      streamingSignatureV4,
      new ByteCounterStream(declaredContentLength, declaredContentLength),
      withReadableStreamHandler(handler),
      { signal }
    )
  }

  return pipeline(body, new ByteCounterStream(limit), withReadableStreamHandler(handler), {
    signal,
  })
}

export default function PutObject(s3Router: S3Router) {
  s3Router.put(
    '/:Bucket/*',
    {
      type: 'iceberg',
      schema: PutObjectInput,
      operation: ROUTE_OPERATIONS.S3_UPLOAD,
      disableContentTypeParser: true,
    },
    async (req, ctx) => {
      const contentLength = ctx.req.streamingSignatureV4
        ? req.Headers['x-amz-decoded-content-length']
        : req.Headers['content-length']
      let key = req.Params['*']

      if (key.endsWith('/') && contentLength === 0) {
        // Consistent with how supabase Storage handles empty folders
        key += '.emptyFolderPlaceholder'
      }

      const uploadRequest = await fileUploadFromRequest(ctx.req, {
        objectName: key,
        allowedMimeTypes: [],
      })

      // We don't trust the params.Bucket sent from the client
      // we utilise the internalIcebergBucketName from the request context
      // to ensure is validated. see http/plugin: iceberg.ts
      const icebergBucket = ctx.req.internalIcebergBucketName

      if (!icebergBucket) {
        throw ERRORS.InvalidParameter('internalIcebergBucketName')
      }

      return pipelineWithOptionalStreamingSignature(
        uploadRequest.body,
        MAX_PART_SIZE,
        ctx.req.streamingSignatureV4,
        contentLength,
        async (fileStream, signal) => {
          const u = await ctx.req.storage.backend.uploadObject(
            icebergBucket,
            key,
            undefined,
            fileStream,
            uploadRequest.mimeType,
            uploadRequest.cacheControl,
            signal,
            uploadRequest.contentLength,
            uploadRequest.contentEncoding
          )

          return {
            headers: {
              etag: u.eTag,
            },
          }
        },
        ctx.signals.body
      )
    }
  )

  s3Router.put(
    '/:Bucket/*',
    {
      schema: PutObjectInput,
      operation: ROUTE_OPERATIONS.S3_UPLOAD,
      disableContentTypeParser: true,
    },
    async (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      const metadata = s3Protocol.parseMetadataHeaders(req.Headers)
      const contentLength = ctx.req.streamingSignatureV4
        ? req.Headers['x-amz-decoded-content-length']
        : req.Headers['content-length']
      let key = req.Params['*']

      if (key.endsWith('/') && contentLength === 0) {
        // Consistent with how supabase Storage handles empty folders
        key += '.emptyFolderPlaceholder'
      }

      const bucket = await ctx.storage
        .asSuperUser()
        .findBucket(req.Params.Bucket, 'id,file_size_limit,allowed_mime_types')

      const uploadRequest = await fileUploadFromRequest(ctx.req, {
        objectName: key,
        allowedMimeTypes: bucket.allowed_mime_types || [],
        fileSizeLimit: bucket.file_size_limit || undefined,
      })

      return pipelineWithOptionalStreamingSignature(
        uploadRequest.body,
        uploadRequest.maxFileSize,
        ctx.req.streamingSignatureV4,
        contentLength,
        async (fileStream, signal) => {
          return s3Protocol.putObject(
            {
              Body: fileStream,
              Bucket: req.Params.Bucket,
              Key: key,
              CacheControl: uploadRequest.cacheControl,
              ContentType: uploadRequest.mimeType,
              ContentLength: uploadRequest.contentLength,
              Expires: req.Headers?.['expires'] ? new Date(req.Headers?.['expires']) : undefined,
              ContentEncoding: req.Headers?.['content-encoding'],
              Metadata: metadata,
            },
            {
              signal,
              isTruncated: uploadRequest.isTruncated,
              declaredContentLength: uploadRequest.declaredContentLength,
            }
          )
        },
        ctx.signals.body
      )
    }
  )

  s3Router.post(
    '/:Bucket|content-type=multipart/form-data',
    {
      schema: PostFormInput,
      operation: ROUTE_OPERATIONS.S3_UPLOAD,
      acceptMultiformData: true,
    },
    async (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      const file = ctx.req.multiPartFileStream

      if (!file) {
        throw ERRORS.InvalidParameter('Missing file')
      }

      const bucket = await ctx.storage
        .asSuperUser()
        .findBucket(req.Params.Bucket, 'id,file_size_limit,allowed_mime_types')

      const fieldsObject = fieldsToObject(file?.fields || {})
      const metadata = s3Protocol.parseMetadataHeaders(fieldsObject)
      const expiresField = fieldsObject.expires

      const maxFileSize = await getStandardMaxFileSizeLimit(ctx.tenantId, bucket.file_size_limit)
      const sizeRange = ctx.req.postPolicyContentLengthRange
      const byteLimit = new ByteCounterStream(
        Math.min(maxFileSize, sizeRange?.max ?? maxFileSize),
        sizeRange?.min
      )

      return pipeline(file.file, byteLimit, async (fileStream) => {
        return s3Protocol.putObject(
          {
            Body: fileStream as stream.Readable,
            Bucket: req.Params.Bucket,
            // biome-ignore lint/suspicious/noTemplateCurlyInString: AWS POST uses a literal ${filename} token
            Key: fieldsObject.key?.replaceAll('${filename}', () => file.filename),
            CacheControl: fieldsObject['cache-control'] as string,
            ContentType: fieldsObject['content-type'] as string,
            Expires: expiresField ? new Date(expiresField) : undefined,
            ContentEncoding: fieldsObject['content-encoding'] as string,
            Metadata: metadata,
          },
          { signal: ctx.signals.body, isTruncated: () => file.file.truncated }
        )
      })
    }
  )
}

function fieldsToObject(fields: MultipartFields) {
  const acc: Record<string, string> = {}

  for (const key in fields) {
    if (!Object.prototype.hasOwnProperty.call(fields, key)) {
      continue
    }

    const field = fields[key]
    if (Array.isArray(field) || !field) {
      continue
    }

    if (
      field.type === 'field' &&
      (typeof field.value === 'string' || field.value === 'number' || field.value === 'boolean')
    ) {
      acc[field.fieldname.toLowerCase()] = field.value
    }
  }

  return acc
}
