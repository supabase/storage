import { ERRORS } from '@internal/errors'
import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'
import { ROUTE_OPERATIONS } from '../../operations'
import { S3Router } from '../router'

const ListObjectsV2Input = {
  summary: 'List Objects V2',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
    },
    required: ['Bucket'],
  },
  Querystring: {
    type: 'object',
    properties: {
      'list-type': { type: 'string', enum: ['2'] },
      delimiter: { type: 'string' },
      'encoding-type': { type: 'string', enum: ['url'] },
      'max-keys': { type: 'integer', minimum: 0, finite: true },
      prefix: { type: 'string' },
      'continuation-token': { type: 'string' },
      'start-after': { type: 'string' },
    },
    required: ['list-type'],
  },
} as const

const ListObjectVersionsInput = {
  summary: 'List Object Versions',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
    },
    required: ['Bucket'],
  },
  Querystring: {
    type: 'object',
    properties: {
      versions: { type: 'string' },
      delimiter: { type: 'string' },
      // encoding-type and max-keys are validated by the handler, which
      // reports them with S3's InvalidArgument messages.
      'encoding-type': { type: 'string' },
      'key-marker': { type: 'string' },
      'max-keys': { type: 'string' },
      prefix: { type: 'string' },
      'version-id-marker': { type: 'string' },
    },
    required: ['versions'],
  },
} as const

const ListObjectsInput = {
  summary: 'List Objects',
  Params: {
    type: 'object',
    properties: {
      Bucket: { type: 'string' },
    },
    required: ['Bucket'],
  },
  Querystring: {
    type: 'object',
    properties: {
      delimiter: { type: 'string' },
      'encoding-type': { type: 'string', enum: ['url'] },
      'max-keys': { type: 'integer', minimum: 0, finite: true },
      prefix: { type: 'string' },
      marker: { type: 'string' },
    },
  },
} as const

function parseEncodingType(encodingType: string | undefined) {
  if (encodingType !== undefined && encodingType !== 'url') {
    throw ERRORS.InvalidArgument('Invalid Encoding Method specified in Request')
  }
  return encodingType
}

function parseMaxKeys(maxKeys: string | undefined) {
  if (maxKeys === undefined) {
    return undefined
  }
  const value = Number(maxKeys)
  if (!/^-?\d+$/.test(maxKeys) || !Number.isSafeInteger(value)) {
    throw ERRORS.InvalidArgument('Provided max-keys not an integer or within integer range')
  }
  if (value < 0) {
    throw ERRORS.InvalidArgument('max-keys cannot be negative')
  }
  return value
}

export default function ListObjects(s3Router: S3Router) {
  // Registered first: ?versions selects ListObjectVersions even when the
  // request also carries list-type=2, as it does in S3.
  s3Router.get(
    '/:Bucket?versions',
    { schema: ListObjectVersionsInput, operation: ROUTE_OPERATIONS.S3_LIST_OBJECT_VERSIONS },
    async (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      return s3Protocol.listObjectVersions({
        Bucket: req.Params.Bucket,
        Prefix: req.Querystring?.prefix || '',
        KeyMarker: req.Querystring?.['key-marker'],
        VersionIdMarker: req.Querystring?.['version-id-marker'],
        EncodingType: parseEncodingType(req.Querystring?.['encoding-type']),
        MaxKeys: parseMaxKeys(req.Querystring?.['max-keys']),
        Delimiter: req.Querystring?.delimiter,
      })
    }
  )

  s3Router.get(
    '/:Bucket?list-type=2',
    { schema: ListObjectsV2Input, operation: ROUTE_OPERATIONS.S3_LIST_OBJECT },
    async (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      return s3Protocol.listObjectsV2({
        Bucket: req.Params.Bucket,
        Prefix: req.Querystring?.prefix || '',
        ContinuationToken: req.Querystring?.['continuation-token'],
        StartAfter: req.Querystring?.['start-after'],
        EncodingType: req.Querystring?.['encoding-type'],
        MaxKeys: req.Querystring?.['max-keys'],
        Delimiter: req.Querystring?.delimiter,
      })
    }
  )

  s3Router.get(
    '/:Bucket',
    { schema: ListObjectsInput, operation: ROUTE_OPERATIONS.S3_LIST_OBJECT },
    async (req, ctx) => {
      const s3Protocol = new S3ProtocolHandler(ctx.storage, ctx.tenantId, ctx.owner)

      return s3Protocol.listObjects({
        Bucket: req.Params.Bucket,
        Prefix: req.Querystring?.prefix || '',
        Marker: req.Querystring?.['marker'],
        EncodingType: req.Querystring?.['encoding-type'],
        MaxKeys: req.Querystring?.['max-keys'],
        Delimiter: req.Querystring?.delimiter,
      })
    }
  )
}
