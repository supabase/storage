import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { S3Router } from '../router'
import { ROUTE_OPERATIONS } from '../../operations'
import { Multipart, MultipartValue } from '@fastify/multipart'
import { fileUploadFromRequest, getStandardMaxFileSizeLimit } from '@storage/uploader'
import { ERRORS } from '@internal/errors'
import { pipeline } from 'stream/promises'
import { ByteLimitTransformStream } from '@storage/protocols/s3/byte-limit-stream'
import stream from 'stream'

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
      'x-amz-date': { type: 'string' },
      'content-type': { type: 'string' },
      'content-length': { type: 'integer' },
      'cache-control': { type: 'string' },
      'content-disposition': { type: 'string' },
      'content-encoding': { type: 'string' },
      expires: { type: 'string' },
    },
    required: ['content-length'],
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

export default function PutObject(s3Router: S3Router) {
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
      const contentLength = req.Headers['content-length']
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

      return s3Protocol.putObject(
        {
          Body: uploadRequest.body,
          Bucket: req.Params.Bucket,
          Key: key,
          CacheControl: uploadRequest.cacheControl,
          ContentType: uploadRequest.mimeType,
          Expires: req.Headers?.['expires'] ? new Date(req.Headers?.['expires']) : undefined,
          ContentEncoding: req.Headers?.['content-encoding'],
          Metadata: metadata,
        },
        { signal: ctx.signals.body, isTruncated: uploadRequest.isTruncated }
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

      const metadata = s3Protocol.parseMetadataHeaders(file?.fields || {})
      const expiresField = normaliseFormDataField(file?.fields?.Expires) as string | undefined

      const maxFileSize = await getStandardMaxFileSizeLimit(ctx.tenantId, bucket.file_size_limit)

      return pipeline(file.file, new ByteLimitTransformStream(maxFileSize), async (fileStream) => {
        return s3Protocol.putObject(
          {
            Body: fileStream as stream.Readable,
            Bucket: req.Params.Bucket,
            Key: normaliseFormDataField(file?.fields?.key) as string,
            CacheControl: normaliseFormDataField(file?.fields?.['Cache-Control']) as string,
            ContentType: normaliseFormDataField(file?.fields?.['Content-Type']) as string,
            Expires: expiresField ? new Date(expiresField) : undefined,
            ContentEncoding: normaliseFormDataField(file?.fields?.['Content-Encoding']) as string,
            Metadata: metadata,
          },
          { signal: ctx.signals.body, isTruncated: () => file.file.truncated }
        )
      })
    }
  )
}

function normaliseFormDataField(value: Multipart | Multipart[] | undefined) {
  if (!value) {
    return undefined
  }

  if (Array.isArray(value)) {
    return (value[0] as MultipartValue).value as string
  }

  if (value.type === 'field') {
    return value.value
  }

  return value.file
}
