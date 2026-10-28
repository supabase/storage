import {
  AbortMultipartUploadCommandInput,
  CompleteMultipartUploadCommandInput,
  CopyObjectCommandInput,
  CreateMultipartUploadCommandInput,
  DeleteObjectCommandInput,
  DeleteObjectsCommandInput,
  GetObjectCommandInput,
  GetObjectTaggingCommandInput,
  HeadObjectCommandInput,
  ListBucketsCommandInput,
  ListMultipartUploadsCommandInput,
  ListObjectsCommandInput,
  ListObjectsV2CommandInput,
  ListObjectsV2Output,
  ListObjectVersionsCommandInput,
  ListPartsCommandInput,
  PutObjectCommandInput,
  UploadPartCommandInput,
  UploadPartCopyCommandInput,
} from '@aws-sdk/client-s3'
import { decrypt, encrypt } from '@internal/auth'
import { ERRORS, ErrorCode, isS3Error, isStorageError } from '@internal/errors'
import { isValidHeader } from '@internal/http/header'
import { logger, logSchema } from '@internal/monitoring'
import { ByteCounterStream } from '@internal/streams'
import { XmlSequence } from '@internal/xml'
import { PassThrough, Readable } from 'stream'
import stream from 'stream/promises'
import { getConfig } from '../../../config'
import type { ObjectResponse } from '../../backend'
import {
  assertLifecycleApiEnabled,
  assertLifecycleWriteReady,
  LifecycleConfigurationValidationError,
  lifecycleConfigurationToS3,
  normalizeS3LifecycleConfiguration,
} from '../../lifecycle'
import { getFileSizeLimit, mustBeValidBucketName, mustBeValidKey } from '../../limits'
import { type DeleteObjectEntry, encodeContinuationToken } from '../../object'
import { parseCopySourceRangeHeader } from '../../range'
import { BucketVersioningStatus, Obj, S3MultipartUpload } from '../../schemas'
import { Storage } from '../../storage'
import { Uploader } from '../../uploader'
import {
  normalizeContentEncoding,
  validateContentEncoding,
} from '../../validators/content-encoding'
import { validateMimeType } from '../../validators/mime-type'
import { encodeRFC3986URIComponent } from './signature-v4'
import {
  deleteMarkerHeaders,
  parseVersionId,
  S3VersionRef,
  toS3VersionId,
  versionIdHeader,
  versionLastModified,
  versionLookup,
} from './versioning'

const { storageS3Region, storageS3Bucket } = getConfig()

function encodeListResponseValue(value: string | undefined, encodingType: string | undefined) {
  return value !== undefined && encodingType === 'url' ? encodeRFC3986URIComponent(value) : value
}

function assertMultipartUploadIdentity(
  upload: Pick<S3MultipartUpload, 'bucket_id' | 'key'>,
  bucket: string | undefined,
  key: string | undefined,
  uploadId: string
) {
  if (upload.bucket_id !== bucket || upload.key !== key) {
    throw ERRORS.NoSuchUpload(uploadId)
  }
}

export function assertPartsAscending(parts: { PartNumber?: number }[]) {
  for (let i = 1; i < parts.length; i++) {
    if ((parts[i].PartNumber ?? 0) <= (parts[i - 1].PartNumber ?? 0)) {
      throw ERRORS.InvalidPartOrder()
    }
  }
}

/**
 * Iceberg buckets are read straight from the backend, which keeps no object
 * versions, so a request there that names a versionId is rejected rather
 * than silently served the only copy.
 */
function rejectVersionId(versionId: string | undefined) {
  if (versionId !== undefined) {
    throw ERRORS.NotSupported('S3 object versioning (versionId)')
  }
}

/** Columns a GET or HEAD needs to serve, or to describe, the row it lands on. */
const READ_OBJECT_COLUMNS =
  'version,metadata,user_metadata,created_at,updated_at,archived_at,is_delete_marker,is_versioned'

/** S3 rejects a malformed copy-source version id as a generic InvalidRequest. */
function parseCopySourceVersionId(versionId: string | undefined) {
  try {
    return parseVersionId(versionId)
  } catch (e) {
    if (isStorageError(ErrorCode.InvalidArgument, e)) {
      throw ERRORS.InvalidRequest('Invalid Request', e)
    }
    throw e
  }
}

/**
 * x-amz-copy-source-version-id: S3 echoes a requested source version and
 * otherwise reports the version a versioned source resolved to.
 */
function copySourceVersionHeader(
  source: Pick<Obj, 'version' | 'is_versioned'> | undefined,
  requestedVersionId: string | undefined
): Record<string, string> {
  if (requestedVersionId !== undefined) {
    return { 'x-amz-copy-source-version-id': requestedVersionId }
  }
  if (source?.is_versioned && source.version) {
    return { 'x-amz-copy-source-version-id': source.version }
  }
  return {}
}

function versionKey(name: string, version: string) {
  return `${name}\0${version}`
}

function withLifecycleErrorMapping<T>(fn: () => T): T {
  try {
    return fn()
  } catch (error) {
    if (error instanceof LifecycleConfigurationValidationError) {
      if (error.category === 'INVALID_ARGUMENT') throw ERRORS.InvalidArgument(error.message, error)
      if (error.category === 'INVALID_REQUEST') throw ERRORS.InvalidRequest(error.message, error)
      throw ERRORS.MalformedXML(error.message, error)
    }
    throw error
  }
}

export const MAX_PART_SIZE = 5 * 1024 * 1024 * 1024 // 5GB

/**
 * Decodes a single URL-encoded segment of the x-amz-copy-source header.
 *
 * Object keys never contain "%", so a malformed escape cannot match a stored
 * object. Keep the raw segment so the request fails as not found instead of
 * throwing a URIError.
 */
function decodeCopySourceSegment(segment: string) {
  try {
    return decodeURIComponent(segment)
  } catch {
    return segment
  }
}

const COPY_SOURCE_VERSION_QUERY = '?versionId='

/**
 * Splits the x-amz-copy-source header into its source bucket, source key and
 * the optional source version id.
 *
 * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
 * Decode before splitting the bucket from the key because the bucket separator
 * can also be encoded. Decode each segment separately to preserve the fallback
 * for malformed escapes without preventing valid segments from being decoded.
 * The only query S3 allows on the header is ?versionId=, and a "?" that belongs
 * to the key arrives encoded, so the version query starts at the first literal
 * "?versionId=".
 */
export function parseCopySource(copySource: string) {
  let rawPath = copySource
  let versionId: string | undefined
  const versionQueryStart = copySource.indexOf(COPY_SOURCE_VERSION_QUERY)
  if (versionQueryStart >= 0) {
    rawPath = copySource.slice(0, versionQueryStart)
    versionId = decodeCopySourceSegment(
      copySource.slice(versionQueryStart + COPY_SOURCE_VERSION_QUERY.length)
    )
  }

  const decodedPath = rawPath.split('/').map(decodeCopySourceSegment).join('/')
  const path = decodedPath.startsWith('/') ? decodedPath.slice(1) : decodedPath
  const separator = path.indexOf('/')

  return {
    bucket: separator < 0 ? path : path.slice(0, separator),
    key: separator < 0 ? '' : path.slice(separator + 1),
    versionId,
  }
}

export class S3ProtocolHandler {
  constructor(
    protected readonly storage: Storage,
    protected readonly tenantId: string,
    protected readonly owner?: string
  ) {}

  /**
   * Returns the versioning state of a bucket. A bucket that never had
   * versioning configured returns an empty configuration, as S3 does.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html
   */
  async getBucketVersioning(bucket: string) {
    const bucketRecord = await this.storage.db
      .asSuperUser()
      .findBucketById(bucket, 'versioning_status')
    const versioningStatus = bucketRecord.versioning_status ?? 'DISABLED'

    return {
      responseBody: {
        VersioningConfiguration:
          versioningStatus === 'DISABLED'
            ? ''
            : { Status: versioningStatus === 'ENABLED' ? 'Enabled' : 'Suspended' },
      },
    }
  }

  /**
   * Sets the versioning state of a bucket to Enabled or Suspended.
   * A configuration without a Status leaves the bucket unchanged.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html
   */
  async putBucketVersioning(
    bucket: string,
    configuration: { Status?: string; MfaDelete?: string } | undefined
  ) {
    if (configuration?.MfaDelete === 'Enabled') {
      throw ERRORS.NotSupported('MFA delete')
    }

    const status = configuration?.Status
    if (status === undefined) {
      await this.storage.findBucket(bucket)
      return {}
    }
    if (status !== 'Enabled' && status !== 'Suspended') {
      throw ERRORS.MalformedXML(
        'The XML you provided was not well-formed or did not validate against our published schema'
      )
    }

    await this.storage.updateBucket(bucket, {
      versioning_status: status === 'Enabled' ? 'ENABLED' : 'SUSPENDED',
    })

    return {}
  }

  async getBucketLifecycle(bucketId: string) {
    assertLifecycleApiEnabled(bucketId)
    const configuration = await this.storage.getBucketLifecycle(bucketId)
    if (!configuration) {
      throw ERRORS.NoSuchLifecycleConfiguration(bucketId)
    }

    return {
      responseBody: lifecycleConfigurationToS3(configuration),
    }
  }

  async putBucketLifecycle(bucketId: string, input: unknown) {
    await assertLifecycleWriteReady(this.storage.db, bucketId)
    await this.storage.putBucketLifecycle(
      bucketId,
      withLifecycleErrorMapping(() => normalizeS3LifecycleConfiguration(input))
    )
    return { statusCode: 200 }
  }

  async deleteBucketLifecycle(bucketId: string) {
    assertLifecycleApiEnabled(bucketId)
    await this.storage.deleteBucketLifecycle(bucketId)
    return { statusCode: 204 }
  }

  /**
   * Returns the Region the bucket resides in
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
   */
  async getBucketLocation() {
    return {
      responseBody: {
        LocationConstraint: storageS3Region ?? '',
      },
    }
  }

  /**
   * Returns a list of all buckets owned by the authenticated sender of the request
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
   */
  async listBuckets(command: ListBucketsCommandInput = {}) {
    const prefix = command.Prefix
    const buckets = await this.storage.listBuckets('name,created_at', { prefix })

    return {
      responseBody: {
        ListAllMyBucketsResult: {
          Buckets: {
            Bucket: buckets.map((bucket) => ({
              Name: bucket.name,
              CreationDate: bucket.created_at
                ? new Date(bucket.created_at || '').toISOString()
                : undefined,
            })),
          },
          ...(prefix !== undefined ? { Prefix: prefix } : {}),
        },
      },
    }
  }

  /**
   * Creates a new S3 bucket.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
   *
   * @param Bucket
   * @param isPublic
   */
  async createBucket(Bucket: string, isPublic: boolean) {
    mustBeValidBucketName(Bucket || '')

    await this.storage.createBucket({
      name: Bucket,
      id: Bucket,
      public: isPublic,
      owner: this.owner,
    })

    return {
      headers: {
        Location: `/${Bucket}`,
      },
    }
  }

  /**
   * Deletes the S3 bucket. All objects in the bucket must be deleted before the bucket itself can be deleted.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
   *
   * @param name
   */
  async deleteBucket(name: string) {
    try {
      await this.storage.deleteBucket(name)
    } catch (e) {
      if (isStorageError(ErrorCode.ResourceNotEmpty, e)) {
        throw ERRORS.S3BucketNotEmpty(name, e)
      }
      throw e
    }

    return {
      statusCode: 204,
    }
  }

  /**
   * You can use this operation to determine if a bucket exists and if you have permission to access it. The action returns a 200 OK if the bucket exists and you have permission to access it.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
   *
   * @param name
   */
  async headBucket(name: string) {
    await this.storage.findBucket(name)
    return {
      statusCode: 200,
      headers: {
        'x-amz-bucket-region': storageS3Region,
      },
    }
  }

  /**
   * Returns some or all (up to 1,000) of the objects in a bucket.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
   * @param command
   */
  async listObjects(command: ListObjectsCommandInput) {
    const list = await this.listObjectsV2({
      Bucket: command.Bucket,
      Delimiter: command.Delimiter,
      EncodingType: command.EncodingType,
      MaxKeys: command.MaxKeys,
      Prefix: command.Prefix,
      StartAfter: command.Marker,
      cursorV1: true,
    })

    const v2Result = list.responseBody.ListBucketResult

    return {
      responseBody: {
        ListBucketResult: {
          Name: v2Result.Name,
          Prefix: v2Result.Prefix,
          Delimiter: v2Result.Delimiter,
          Marker: v2Result.StartAfter,
          ...(v2Result.IsTruncated && v2Result.NextContinuationToken && command.Delimiter
            ? { NextMarker: v2Result.NextContinuationToken }
            : {}),
          MaxKeys: v2Result.MaxKeys,
          IsTruncated: v2Result.IsTruncated,
          Contents: v2Result.Contents,
          CommonPrefixes: v2Result.CommonPrefixes,
          EncodingType: v2Result.EncodingType,
        },
      },
    }
  }

  /**
   * List objects in a bucket, implements the ListObjectsV2Command
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
   *
   * @param command
   */
  async listObjectsV2(command: ListObjectsV2CommandInput & { cursorV1?: boolean }) {
    if (!command.Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    await this.storage.asSuperUser().findBucket(command.Bucket)

    const continuationToken = command.ContinuationToken
    const startAfter = command.StartAfter
    const encodingType = command.EncodingType
    const delimiter = command.Delimiter
    const prefix = command.Prefix || ''
    const maxKeys = command.MaxKeys
    const bucket = command.Bucket

    const limit = Math.min(maxKeys ?? 1000, 1000)

    const results =
      limit === 0
        ? {
            folders: [],
            objects: [],
            hasNext: false,
            nextCursor: undefined,
            nextCursorKey: undefined,
          }
        : await this.storage.from(bucket).listObjectsV2({
            prefix,
            delimiter,
            maxKeys: limit,
            cursor: continuationToken,
            startAfter,
            s3Compatible: true,
          })

    const commonPrefixes: { Prefix: string }[] = []
    for (const object of results.folders) {
      commonPrefixes.push({
        Prefix: encodeListResponseValue(object.name, encodingType) as string,
      })
    }

    const contents: NonNullable<ListObjectsV2Output['Contents']> = []
    for (const o of results.objects) {
      contents.push({
        Key: encodeListResponseValue(o.name, encodingType),
        LastModified: (o.updated_at ? new Date(o.updated_at).toISOString() : undefined) as
          | Date
          | undefined,
        ETag: o.metadata?.eTag as string,
        Size: (o.metadata?.size as number) || 0,
        StorageClass: 'STANDARD' as const,
      })
    }

    const response: { ListBucketResult: ListObjectsV2Output } = {
      ListBucketResult: {
        Name: bucket,
        Prefix: encodeListResponseValue(prefix, encodingType),
        ContinuationToken: continuationToken,
        StartAfter: encodeListResponseValue(startAfter, encodingType),
        Contents: contents,
        IsTruncated: results.hasNext,
        MaxKeys: limit,
        Delimiter: encodeListResponseValue(delimiter, encodingType),
        EncodingType: encodingType,
        KeyCount: results.objects.length + results.folders.length,
        CommonPrefixes: commonPrefixes,
      },
    }

    if (results.nextCursor) {
      if (command.cursorV1) {
        response.ListBucketResult.NextContinuationToken = encodeListResponseValue(
          results.nextCursorKey,
          encodingType
        )
      } else {
        response.ListBucketResult.NextContinuationToken = results.nextCursor
      }
    }

    return {
      responseBody: response,
    }
  }

  /**
   * Lists every version and delete marker of the objects in a bucket, keys
   * ascending and each key's versions newest first. Versions and delete
   * markers come back interleaved in that one order. A truncated page names
   * its last entry as the next key/version-id marker, and the listing resumes
   * strictly after it.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
   *
   * @param command
   */
  async listObjectVersions(command: ListObjectVersionsCommandInput) {
    if (!command.Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    const bucket = command.Bucket
    const encodingType = command.EncodingType
    const delimiter = command.Delimiter
    const prefix = command.Prefix || ''
    const keyMarker = command.KeyMarker || ''
    const versionIdMarker = command.VersionIdMarker

    if (versionIdMarker !== undefined) {
      if (!keyMarker) {
        throw ERRORS.InvalidArgument(
          'A version-id marker cannot be specified without a key marker.'
        )
      }
      if (versionIdMarker === '') {
        throw ERRORS.InvalidArgument('A version-id marker cannot be empty.')
      }
    }
    const versionMarker = parseVersionId(versionIdMarker)

    await this.storage.asSuperUser().findBucket(bucket)

    const maxKeys = command.MaxKeys ?? 1000
    const limit = Math.min(maxKeys, 1000)

    // The version-id marker resumes inside the key-marker key, after that
    // version: the position listObjectsV2's own continuation token encodes,
    // so the marker is turned into one. A marker naming no version of the key
    // resumes after the whole key, via startAfter.
    let cursor: string | undefined
    if (keyMarker && versionMarker) {
      const lookup = versionLookup(versionMarker)
      const marker = await this.storage.db.findObject(
        bucket,
        keyMarker,
        'version,archived_at',
        { ...lookup.filters, dontErrorOnEmpty: true },
        lookup.version
      )
      if (marker?.version) {
        cursor = encodeContinuationToken({
          startAfter: keyMarker,
          afterArchivedAt: marker.archived_at
            ? new Date(marker.archived_at).toISOString()
            : 'infinity',
          afterVersion: marker.version,
          noncurrentVersions: 'include',
          deleteMarkers: 'include',
        })
      }
    }

    const results =
      limit === 0
        ? { folders: [], objects: [], hasNext: false, lastEntry: undefined }
        : await this.storage.from(bucket).listObjectsV2({
            prefix,
            delimiter,
            maxKeys: limit,
            cursor,
            startAfter: keyMarker || undefined,
            noncurrentVersions: 'include',
            deleteMarkers: 'include',
          })

    const entries: [string, unknown][] = []
    for (const object of results.objects) {
      const entry = {
        Key: encodeListResponseValue(object.name, encodingType),
        VersionId: toS3VersionId(object),
        IsLatest: !object.archived_at,
        LastModified: versionLastModified(object)?.toISOString(),
      }

      if (object.is_delete_marker) {
        entries.push(['DeleteMarker', entry])
      } else {
        entries.push([
          'Version',
          {
            ...entry,
            ETag: object.metadata?.eTag as string,
            Size: (object.metadata?.size as number) || 0,
            StorageClass: 'STANDARD',
          },
        ])
      }
    }

    const commonPrefixes: { Prefix: string }[] = []
    for (const folder of results.folders) {
      commonPrefixes.push({ Prefix: encodeListResponseValue(folder.name, encodingType) as string })
    }

    // A truncated page names its last entry as the next markers. A page that
    // ends on a common prefix resumes after the whole prefix, so it names no
    // version.
    let nextKeyMarker: string | undefined
    let nextVersionIdMarker: string | undefined
    const last = results.lastEntry
    if (last) {
      nextKeyMarker = encodeListResponseValue(last.name, encodingType)
      if (last.id !== null) {
        nextVersionIdMarker = toS3VersionId(last)
      }
    }

    return {
      responseBody: {
        ListVersionsResult: {
          Name: bucket,
          Prefix: encodeListResponseValue(prefix, encodingType),
          KeyMarker: encodeListResponseValue(keyMarker, encodingType),
          VersionIdMarker: versionIdMarker ?? '',
          NextKeyMarker: nextKeyMarker,
          NextVersionIdMarker: nextVersionIdMarker,
          MaxKeys: maxKeys,
          Delimiter: encodeListResponseValue(delimiter, encodingType),
          EncodingType: encodingType,
          IsTruncated: results.hasNext,
          Entries: new XmlSequence(entries),
          CommonPrefixes: commonPrefixes,
        },
      },
    }
  }

  /**
   * This operation lists in-progress multipart uploads in a bucket.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
   *
   * @param command
   */
  async listMultipartUploads(command: ListMultipartUploadsCommandInput) {
    if (!command.Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    const limit = command.MaxUploads ?? 1000
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
      throw ERRORS.InvalidParameter('MaxUploads')
    }

    await this.storage.asSuperUser().findBucket(command.Bucket)

    const keyContinuationToken = command.KeyMarker
    const uploadContinuationToken = command.UploadIdMarker

    const encodingType = command.EncodingType
    const delimiter = command.Delimiter
    const prefix = command.Prefix || ''
    const bucket = command.Bucket

    const multipartUploads = await this.storage.db.listMultipartUploads(bucket, {
      prefix,
      deltimeter: delimiter,
      maxKeys: limit + 1,
      nextUploadKeyToken: keyContinuationToken,
      nextUploadToken: uploadContinuationToken,
    })

    let results: Partial<S3MultipartUpload & { isFolder: boolean }>[] = multipartUploads
    let prevPrefix = ''

    if (delimiter) {
      const delimitedResults: Partial<S3MultipartUpload & { isFolder: boolean }>[] = []
      for (const object of multipartUploads) {
        let idx = object.key.slice(prefix.length).indexOf(delimiter)

        if (idx >= 0) {
          idx = prefix.length + idx + delimiter.length
          const currPrefix = object.key.substring(0, idx)
          if (currPrefix === prevPrefix) {
            continue
          }
          prevPrefix = currPrefix
          delimitedResults.push({
            isFolder: true,
            id: object.id,
            key: currPrefix,
            bucket_id: bucket,
          })
          continue
        }

        delimitedResults.push(object)
      }
      results = delimitedResults
    }

    const isTruncated = results.length > limit
    const resultCount = isTruncated ? limit : results.length

    const commonPrefixes: { Prefix: string | undefined }[] = []
    const uploads: {
      Key: string | undefined
      Initiated: string | undefined
      UploadId: string | undefined
      StorageClass: 'STANDARD'
    }[] = []

    for (let index = 0; index < resultCount; index++) {
      const object = results[index]

      if (object.isFolder) {
        commonPrefixes.push({
          Prefix: encodeListResponseValue(object.key, encodingType),
        })
      } else {
        uploads.push({
          Key: encodeListResponseValue(object.key, encodingType),
          Initiated: object.created_at ? new Date(object.created_at).toISOString() : undefined,
          UploadId: object.id,
          StorageClass: 'STANDARD',
        })
      }
    }

    let keyNextContinuationToken: string | undefined
    let uploadNextContinuationToken: string | undefined

    if (isTruncated) {
      const lastItem = results[resultCount - 1]
      keyNextContinuationToken = encodeListResponseValue(lastItem.key, encodingType)
      uploadNextContinuationToken = lastItem.id
    }

    const response = {
      ListMultipartUploadsResult: {
        Bucket: bucket,
        Prefix: encodeListResponseValue(prefix, encodingType),
        KeyMarker: encodeListResponseValue(keyContinuationToken, encodingType),
        UploadIdMarker: uploadContinuationToken,
        NextKeyMarker: keyNextContinuationToken,
        NextUploadIdMarker: uploadNextContinuationToken,
        Upload: uploads,
        IsTruncated: isTruncated,
        MaxUploads: limit,
        Delimiter: encodeListResponseValue(delimiter, encodingType),
        EncodingType: encodingType,
        CommonPrefixes: commonPrefixes,
      },
    }

    return {
      responseBody: response,
    }
  }

  /**
   * This action initiates a multipart upload and returns an upload ID
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
   *
   * @param command
   */
  async createMultiPartUpload(command: CreateMultipartUploadCommandInput) {
    const uploader = new Uploader(this.storage.backend, this.storage.db, this.storage.location)
    const { Bucket, Key } = command
    const contentEncoding = validateContentEncoding(command.ContentEncoding)
    const metadata = { mimetype: command.ContentType, contentEncoding }

    mustBeValidBucketName(Bucket)
    mustBeValidKey(Key)

    const bucket = await this.storage.asSuperUser().findBucket(Bucket, 'id,allowed_mime_types')

    if (command.ContentType && bucket.allowed_mime_types && bucket.allowed_mime_types.length > 0) {
      validateMimeType(command.ContentType, bucket.allowed_mime_types || [])
    }

    // Create Multi Part Upload
    const version = await uploader.prepareUpload({
      bucketId: command.Bucket as string,
      objectName: command.Key as string,
      isUpsert: true,
      owner: this.owner,
      userMetadata: command.Metadata,
      metadata,
      uploadType: 's3',
    })

    const uploadId = await this.storage.backend.createMultiPartUpload(
      storageS3Bucket,
      this.storage.location.getKeyLocation({
        bucketId: command.Bucket as string,
        objectName: command.Key as string,
        tenantId: this.tenantId,
      }),
      version,
      command.ContentType || '',
      command.CacheControl || '',
      undefined,
      contentEncoding
    )

    if (!uploadId) {
      throw ERRORS.InvalidUploadId(uploadId)
    }

    const signature = this.uploadSignature({ in_progress_size: 0 })
    await this.storage.db
      .asSuperUser()
      .createMultipartUpload(
        uploadId,
        Bucket,
        Key,
        version,
        signature,
        this.owner,
        command.Metadata,
        metadata
      )

    return {
      responseBody: {
        InitiateMultipartUploadResult: {
          Bucket: command.Bucket,
          Key: `${command.Key}`,
          UploadId: uploadId,
        },
      },
    }
  }

  /**
   * Completes a multipart upload by assembling previously uploaded parts.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
   *
   * @param command
   */
  async completeMultiPartUpload(command: CompleteMultipartUploadCommandInput) {
    const uploader = new Uploader(this.storage.backend, this.storage.db, this.storage.location)
    const { Bucket, Key, UploadId } = command

    if (!UploadId) {
      throw ERRORS.InvalidUploadId()
    }

    const multiPartUpload = await this.storage.db
      .asSuperUser()
      .findMultipartUpload(UploadId, 'id,version,user_metadata,metadata,bucket_id,key')

    assertMultipartUploadIdentity(multiPartUpload, Bucket, Key, UploadId)

    await uploader.canUpload({
      bucketId: Bucket as string,
      objectName: Key as string,
      isUpsert: true,
      owner: this.owner,
      userMetadata: multiPartUpload.user_metadata || undefined,
      metadata: multiPartUpload.metadata || undefined,
    })

    const parts = command.MultipartUpload?.Parts || []

    assertPartsAscending(parts)

    if (parts.length === 0) {
      const allParts = await this.storage.db.asSuperUser().listParts(UploadId, {
        maxParts: 10000,
      })

      parts.push(
        ...allParts.map((part) => ({
          PartNumber: part.part_number,
          ETag: part.etag,
        }))
      )
    }

    const resp = await this.storage.backend.completeMultipartUpload(
      storageS3Bucket,
      this.storage.location.getKeyLocation({
        bucketId: Bucket as string,
        objectName: Key as string,
        tenantId: this.tenantId,
      }),
      UploadId as string,
      multiPartUpload.version,
      parts,
      { removePrefix: true }
    )

    const metadata = await this.storage.backend.headObject(
      storageS3Bucket,
      this.storage.location.getKeyLocation({
        bucketId: Bucket as string,
        objectName: Key as string,
        tenantId: this.tenantId,
      }),
      resp.version
    )

    const completed = await uploader.completeUpload({
      bucketId: Bucket as string,
      objectName: Key as string,
      version: resp.version,
      isUpsert: true,
      uploadType: 's3',
      objectMetadata: metadata,
      owner: this.owner,
      userMetadata: multiPartUpload.user_metadata || undefined,
    })

    await this.storage.db.asSuperUser().deleteMultipartUpload(UploadId)

    return {
      headers: versionIdHeader(completed.obj),
      responseBody: {
        CompleteMultipartUploadResult: {
          Location: `${Bucket}/${Key}`,
          Bucket,
          Key,
          ChecksumCRC32: resp.ChecksumCRC32,
          ChecksumCRC32C: resp.ChecksumCRC32C,
          ChecksumSHA1: resp.ChecksumSHA1,
          ChecksumSHA256: resp.ChecksumSHA256,
          ETag: resp.ETag,
        },
      },
    }
  }

  /**
   * Uploads a part in a multipart upload.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
   * @param command
   * @param signal
   */
  async uploadPart(command: UploadPartCommandInput, { signal }: { signal?: AbortSignal }) {
    if (signal?.aborted) {
      throw ERRORS.AbortedTerminate('UploadPart aborted')
    }

    const { Bucket, PartNumber, UploadId, Key, Body, ContentLength } = command

    if (!UploadId) {
      throw ERRORS.InvalidUploadId()
    }

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (typeof ContentLength === 'undefined') {
      throw ERRORS.MissingContentLength()
    }

    const multipartData = await this.storage.db
      .asSuperUser()
      .findMultipartUpload(UploadId, 'version,user_metadata,metadata,bucket_id,key')

    assertMultipartUploadIdentity(multipartData, Bucket, Key, UploadId)

    const bucket = await this.storage.asSuperUser().findBucket(Bucket, 'file_size_limit')
    const maxFileSize = await getFileSizeLimit(this.storage.db.tenantId, bucket?.file_size_limit)

    const uploader = new Uploader(this.storage.backend, this.storage.db, this.storage.location)

    await uploader.canUpload({
      bucketId: Bucket as string,
      objectName: Key as string,
      owner: this.owner,
      isUpsert: true,
      userMetadata: multipartData.user_metadata || undefined,
      metadata: multipartData.metadata || undefined,
    })

    const multipart = await this.shouldAllowPartUpload(UploadId, ContentLength, maxFileSize)

    if (signal?.aborted) {
      throw ERRORS.AbortedTerminate('UploadPart aborted')
    }

    const proxy = new PassThrough()

    if (Body instanceof Readable) {
      proxy.on('error', () => {
        Body.unpipe(proxy)
      })

      Body.on('error', (err) => {
        if (!proxy.closed) {
          proxy.destroy(err)
        }
      })
    }

    const body = Body instanceof Readable ? Body.pipe(proxy) : Readable.from(Body as Buffer)

    try {
      const uploadPart = await stream.pipeline(
        body,
        new ByteCounterStream(ContentLength, ContentLength),
        async (stream, options) => {
          return this.storage.backend.uploadPart(
            storageS3Bucket,
            this.storage.location.getKeyLocation({
              bucketId: Bucket as string,
              objectName: Key as string,
              tenantId: this.tenantId,
            }),
            multipart.version,
            UploadId,
            PartNumber || 0,
            stream as Readable,
            ContentLength,
            options?.signal
          )
        },
        { signal }
      )

      await this.storage.db.asSuperUser().insertUploadPart({
        upload_id: UploadId,
        version: multipart.version,
        part_number: PartNumber || 0,
        size: ContentLength,
        etag: uploadPart.ETag || '',
        key: Key as string,
        bucket_id: Bucket,
        owner_id: this.owner,
      })

      return {
        headers: {
          etag: uploadPart.ETag || '',
          'Access-Control-Expose-Headers': 'etag',
        },
      }
    } catch (e) {
      try {
        await this.storage.db.asSuperUser().withTransaction(async (db) => {
          const multipart = await db.findMultipartUpload(UploadId, 'in_progress_size', {
            forUpdate: true,
          })

          const diff = multipart.in_progress_size - ContentLength
          const signature = this.uploadSignature({ in_progress_size: diff })
          await db.updateMultipartUploadProgress(UploadId, diff, signature)
        })
      } catch (e) {
        logSchema.error(logger, 'Failed to update multipart upload progress', {
          type: 's3',
          error: e,
        })
      }

      if (e instanceof Error && e.name === 'AbortError') {
        throw ERRORS.AbortedTerminate('UploadPart aborted')
      }

      throw e
    }
  }

  /**
   * Adds an object to a bucket.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
   *
   * @param command
   * @param options
   */
  async putObject(
    command: PutObjectCommandInput,
    options: {
      signal?: AbortSignal
      isTruncated: () => boolean
      declaredContentLength?: number
    }
  ) {
    const uploader = new Uploader(this.storage.backend, this.storage.db, this.storage.location)
    const contentEncoding = validateContentEncoding(command.ContentEncoding)

    mustBeValidBucketName(command.Bucket)
    mustBeValidKey(command.Key)

    const upload = await uploader.upload({
      bucketId: command.Bucket as string,
      file: {
        body: command.Body as Readable,
        cacheControl: command.CacheControl!,
        contentEncoding,
        mimeType: command.ContentType!,
        contentLength: command.ContentLength,
        declaredContentLength: options.declaredContentLength,
        isTruncated: options.isTruncated,
      },
      objectName: command.Key as string,
      userMetadata: command.Metadata,
      owner: this.owner,
      isUpsert: true,
      uploadType: 's3',
      signal: options.signal,
    })

    return {
      headers: {
        etag: upload.metadata.eTag,
        ...versionIdHeader(upload.obj),
      },
    }
  }

  /**
   * This operation aborts a multipart upload
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
   *
   * @param command
   */
  async abortMultipartUpload(command: AbortMultipartUploadCommandInput) {
    const { Bucket, Key, UploadId } = command

    if (!UploadId) {
      throw ERRORS.InvalidUploadId()
    }

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (!Key) {
      throw ERRORS.MissingParameter('Key')
    }

    const multipart = await this.storage.db
      .asSuperUser()
      .findMultipartUpload(UploadId, 'id,version,user_metadata,metadata,bucket_id,key')

    assertMultipartUploadIdentity(multipart, Bucket, Key, UploadId)

    const uploader = new Uploader(this.storage.backend, this.storage.db, this.storage.location)
    await uploader.canUpload({
      bucketId: Bucket,
      objectName: Key,
      owner: this.owner,
      isUpsert: true,
      userMetadata: multipart.user_metadata || undefined,
      metadata: multipart.metadata || undefined,
    })

    try {
      await this.storage.backend.abortMultipartUpload(
        storageS3Bucket,
        this.storage.location.getKeyLocation({
          bucketId: Bucket,
          objectName: Key,
          tenantId: this.tenantId,
        }),
        UploadId,
        multipart.version
      )
    } catch (e) {
      // gracefully continue if the upload part was already deleted/aborted on the S3 side
      // error.name: NoSuchUpload
      // error.message: The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.
      if (!isS3Error(e) || e.name !== 'NoSuchUpload') {
        throw e
      }
    }

    await this.storage.db.asSuperUser().deleteMultipartUpload(UploadId)

    return {}
  }

  async headObject(command: HeadObjectCommandInput) {
    const { Bucket, Key } = command

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (!Key) {
      throw ERRORS.MissingParameter('Key')
    }

    rejectVersionId(command.VersionId)

    const r = await this.storage.backend.headObject(Bucket, Key, undefined)

    return {
      headers: {
        'cache-control': r.cacheControl || '',
        'content-encoding': r.contentEncoding || '',
        'content-length': r.contentLength?.toString() || '0',
        'content-type': r.mimetype || '',
        etag: r.eTag || '',
        'last-modified': r.lastModified?.toUTCString() || '',
      },
    }
  }

  /**
   * The HEAD operation retrieves metadata from an object without returning the object itself. This operation is useful if you're interested only in an object's metadata.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
   *
   * @param command
   * @param opts
   */
  async dbHeadObject(command: HeadObjectCommandInput) {
    const { Bucket, Key } = command

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (!Key) {
      throw ERRORS.MissingParameter('Key')
    }

    const { object, versioningStatus } = await this.findReadableObject(
      Bucket,
      Key,
      command.VersionId
    )

    let metadataHeaders: Record<string, unknown> = {}

    if (object.user_metadata) {
      metadataHeaders = toAwsMetadataHeaders(object.user_metadata)
    }

    return {
      headers: {
        'created-at': (object.created_at as string) || '',
        'cache-control': (object.metadata?.cacheControl as string) || '',
        'content-encoding': (object.metadata?.contentEncoding as string) || '',
        expires: (object.metadata?.expires as string) || '',
        'content-length': String(object.metadata?.size ?? ''),
        'content-type': (object.metadata?.mimetype as string) || '',
        etag: (object.metadata?.eTag as string) || '',
        'last-modified': versionLastModified(object)?.toUTCString() || '',
        ...versionIdHeader(object, versioningStatus),
        ...metadataHeaders,
      },
    }
  }

  /**
   * Finds the row a GET or HEAD reads: the current row, or the version the
   * request names. A delete marker answers as S3 does: 404 NoSuchKey when it
   * is the current row, 405 MethodNotAllowed when addressed by version id,
   * both carrying the marker's headers.
   *
   * Also returns the bucket's versioning status, which decides whether a
   * null version reports x-amz-version-id: null. It is read as superuser, as
   * the caller may read objects without being allowed to read the bucket
   * row, and in parallel with the object so reads are not slower. A missing
   * bucket is reported before a missing object, as S3 does.
   */
  protected async findReadableObject(bucket: string, key: string, versionId: string | undefined) {
    const ref = parseVersionId(versionId)
    const lookup = versionLookup(ref)
    const [bucketLookup, objectLookup] = await Promise.allSettled([
      this.storage.asSuperUser().findBucket(bucket, 'versioning_status'),
      this.storage
        .from(bucket)
        .findObject(
          key,
          READ_OBJECT_COLUMNS,
          { ...lookup.filters, excludeDeleteMarkers: false, dontErrorOnEmpty: true },
          lookup.version
        ),
    ])
    if (bucketLookup.status === 'rejected') {
      throw bucketLookup.reason
    }
    if (objectLookup.status === 'rejected') {
      throw objectLookup.reason
    }
    const object = objectLookup.value
    const versioningStatus = bucketLookup.value.versioning_status

    if (!object) {
      throw ref ? ERRORS.NoSuchVersion(key) : ERRORS.NoSuchKey(key)
    }

    if (object.is_delete_marker) {
      if (!ref) {
        throw ERRORS.NoSuchKey(key).withResponseHeaders(deleteMarkerHeaders(object))
      }
      const headers: Record<string, string> = { ...deleteMarkerHeaders(object), allow: 'DELETE' }
      const lastModified = versionLastModified(object)
      if (lastModified) {
        headers['last-modified'] = lastModified.toUTCString()
      }
      throw ERRORS.MethodNotAllowed(key).withResponseHeaders(headers)
    }

    return { object, versioningStatus }
  }

  async getObjectTagging(command: GetObjectTaggingCommandInput) {
    const { Bucket, Key } = command

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (!Key) {
      throw ERRORS.MissingParameter('Key')
    }

    const object = await this.storage.from(Bucket).findObject(Key, 'id')

    if (!object) {
      throw ERRORS.NoSuchKey(Key)
    }

    // TODO: implement tagging when supported
    return {
      responseBody: {
        Tagging: {
          TagSet: null,
        },
      },
    }
  }

  /**
   * Retrieves an object from Amazon S3.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
   *
   * @param command
   * @param options
   */
  async getObject(
    command: GetObjectCommandInput,
    options?: { skipDbCheck?: boolean; signal?: AbortSignal }
  ) {
    const bucket = command.Bucket as string
    const key = command.Key as string

    let object: Obj | undefined
    let versioningStatus: BucketVersioningStatus | undefined
    if (options?.skipDbCheck) {
      rejectVersionId(command.VersionId)
    } else {
      const readable = await this.findReadableObject(bucket, key, command.VersionId)
      object = readable.object
      versioningStatus = readable.versioningStatus
    }

    const version = object?.version
    const userMetadata = object?.user_metadata

    let response: ObjectResponse
    try {
      response = await this.storage.backend.getObject(
        this.storage.location.getRootLocation(),
        this.storage.location.getKeyLocation({
          bucketId: bucket,
          objectName: key,
          tenantId: this.tenantId,
        }),
        version,
        {
          ifMatch: command.IfMatch,
          ifModifiedSince: command.IfModifiedSince?.toISOString(),
          ifNoneMatch: command.IfNoneMatch,
          ifUnmodifiedSince: command.IfUnmodifiedSince?.toISOString(),
          range: command.Range,
        },
        options?.signal
      )
    } catch (error) {
      // The S3 SDK rejects on a 304; the file backend returns it as a value.
      if (isS3Error(error) && error.$metadata.httpStatusCode === 304) {
        const upstream = error.$response?.headers
        const headers: Record<string, string> = {
          'cache-control': upstream?.['cache-control'] || '',
          'content-encoding': normalizeContentEncoding(upstream?.['content-encoding']) || '',
          etag: upstream?.etag || '',
          'last-modified': upstream?.['last-modified'] || '',
          ...versionIdHeader(object, versioningStatus),
        }
        return { headers, responseBody: undefined, statusCode: 304 }
      }
      throw error
    }

    let metadataHeaders: Record<string, unknown> = {}

    if (userMetadata) {
      metadataHeaders = toAwsMetadataHeaders(userMetadata)
    }

    const headers: Record<string, string> = {
      'cache-control': response.metadata.cacheControl,
      'content-encoding': response.metadata.contentEncoding || '',
      'content-length':
        (response.httpStatusCode === 304
          ? response.metadata.size
          : response.metadata.contentLength
        )?.toString() || '0',
      'content-range': response.metadata.contentRange?.toString() || '',
      'content-type': response.metadata.mimetype,
      etag: response.metadata.eTag,
      'last-modified': response.metadata.lastModified?.toUTCString() || '',
      ...versionIdHeader(object, versioningStatus),
      ...metadataHeaders,
    }

    // Handle response header overrides
    if (
      command.ResponseContentDisposition &&
      isValidHeader('content-disposition', command.ResponseContentDisposition)
    ) {
      headers['content-disposition'] = command.ResponseContentDisposition
    }
    if (command.ResponseContentType && isValidHeader('content-type', command.ResponseContentType)) {
      headers['content-type'] = command.ResponseContentType
    }
    if (
      command.ResponseCacheControl &&
      isValidHeader('cache-control', command.ResponseCacheControl)
    ) {
      headers['cache-control'] = command.ResponseCacheControl
    }
    if (
      command.ResponseContentEncoding &&
      isValidHeader('content-encoding', command.ResponseContentEncoding)
    ) {
      headers['content-encoding'] = command.ResponseContentEncoding
    }
    if (
      command.ResponseContentLanguage &&
      isValidHeader('content-language', command.ResponseContentLanguage)
    ) {
      headers['content-language'] = command.ResponseContentLanguage
    }
    if (command.ResponseExpires) {
      headers['expires'] = command.ResponseExpires.toUTCString()
    }

    return {
      headers,
      responseBody: response.body,
      statusCode: response.httpStatusCode,
    }
  }

  /**
   * Removes an object from a bucket. Without a versionId this deletes the
   * current row, or writes a delete marker when the bucket is versioned.
   * With one it permanently deletes that version; a version that does not
   * exist is a successful no-op, as in S3.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
   *
   * @param command
   */
  async deleteObject(command: DeleteObjectCommandInput) {
    const { Bucket, Key } = command

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (!Key) {
      throw ERRORS.MissingParameter('Key')
    }

    const ref = parseVersionId(command.VersionId)
    const headers: Record<string, string> = {}
    if (command.VersionId !== undefined) {
      headers['x-amz-version-id'] = command.VersionId
    }

    // The version is resolved as superuser: whether the caller may delete it
    // is decided by the delete itself, against the DELETE policy.
    let target: Obj | undefined
    if (ref) {
      const lookup = versionLookup(ref)
      target = await this.storage
        .asSuperUser()
        .from(Bucket)
        .findObject(
          Key,
          'version',
          { ...lookup.filters, excludeDeleteMarkers: false, dontErrorOnEmpty: true },
          lookup.version
        )

      if (!target) {
        await this.storage.asSuperUser().findBucket(Bucket)
        return { statusCode: 204, headers }
      }
    }

    let deleted: Obj | undefined
    try {
      deleted = await this.storage
        .from(Bucket)
        .deleteObject(Key, target?.version, { owner: this.owner })
    } catch (e) {
      if (!isStorageError(ErrorCode.NoSuchKey, e)) {
        throw e
      }

      await this.storage.asSuperUser().findBucket(Bucket)
    }

    if (deleted?.is_delete_marker) {
      if (ref) {
        // A delete by version id removed a marker.
        headers['x-amz-delete-marker'] = 'true'
      } else {
        // A simple delete wrote a marker.
        Object.assign(headers, deleteMarkerHeaders(deleted))
      }
    }

    return {
      statusCode: 204,
      headers,
    }
  }

  /**
   * This operation enables you to delete multiple objects from a bucket using a single HTTP request.
   *
   * An entry without a VersionId deletes the current row, or writes a delete
   * marker when the bucket is versioned. An entry with one permanently
   * deletes that version, and reports success when there is no such version.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
   *
   * @param command
   */
  async deleteObjects(command: DeleteObjectsCommandInput) {
    const { Bucket, Delete } = command

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (!Delete) {
      throw ERRORS.MissingParameter('Delete')
    }

    if (!Array.isArray(Delete.Objects)) {
      throw ERRORS.InvalidParameter('Objects')
    }

    if (Delete.Objects.length === 0) {
      await this.storage.asSuperUser().findBucket(Bucket)
      return { responseBody: { DeleteResult: { Deleted: [], Error: [] } } }
    }

    const targets = Delete.Objects.map((object) => {
      try {
        return { object, ref: parseVersionId(object.VersionId), invalidVersion: false }
      } catch (e) {
        if (!isStorageError(ErrorCode.InvalidArgument, e)) {
          throw e
        }
        return { object, ref: undefined, invalidVersion: true }
      }
    })

    // Resolve each named version to its row as superuser: a version that does
    // not exist is reported deleted without touching anything, and whether
    // the caller may delete one that does is decided by the delete itself.
    const idTargets: { name: string; version: string }[] = []
    const nullVersionNames: string[] = []
    for (const { object, ref } of targets) {
      if (object.Key === undefined || !ref) {
        continue
      }
      if (ref.kind === 'id') {
        idTargets.push({ name: object.Key, version: ref.version })
      } else {
        nullVersionNames.push(object.Key)
      }
    }

    const versionRows = new Map<string, Obj>()
    if (idTargets.length > 0) {
      const rows = await this.storage.db
        .asSuperUser()
        .findObjectVersions(Bucket, idTargets, 'name,version,is_versioned')
      for (const row of rows) {
        if (row.is_versioned && row.version) {
          versionRows.set(versionKey(row.name, row.version), row)
        }
      }
    }
    const nullVersionRows = new Map<string, Obj>()
    if (nullVersionNames.length > 0) {
      const rows = await this.storage.db
        .asSuperUser()
        .findNullVersions(Bucket, nullVersionNames, 'name,version')
      for (const row of rows) {
        nullVersionRows.set(row.name, row)
      }
    }

    const resolveVersion = (key: string, ref: S3VersionRef) => {
      if (ref.kind === 'id') {
        return versionRows.get(versionKey(key, ref.version))
      }
      return nullVersionRows.get(key)
    }

    const entries: DeleteObjectEntry[] = []
    const requestedKeys: string[] = []
    for (const { object, ref, invalidVersion } of targets) {
      if (object.Key === undefined || invalidVersion) {
        continue
      }
      if (!ref) {
        requestedKeys.push(object.Key || '')
        entries.push(object.Key || '')
        continue
      }
      const row = resolveVersion(object.Key, ref)
      if (row?.version) {
        entries.push({ path: row.name, versionId: row.version })
      }
    }

    const applied =
      entries.length > 0
        ? await this.storage.from(Bucket).deleteObjects(entries, { owner: this.owner })
        : { plain: [], versioned: [] }

    const plainDeleted = new Map<string, Obj>()
    for (const row of applied.plain) {
      plainDeleted.set(row.name, row)
    }
    const versionsDeleted = new Map<string, Obj>()
    for (const row of applied.versioned) {
      if (row.version) {
        versionsDeleted.set(versionKey(row.name, row.version), row)
      }
    }

    const unresolvedKeys: string[] = []
    for (const key of requestedKeys) {
      if (!plainDeleted.has(key)) {
        unresolvedKeys.push(key)
      }
    }

    const remainingObjects =
      unresolvedKeys.length > 0
        ? await this.storage.asSuperUser().from(Bucket).findObjects(unresolvedKeys, 'name')
        : []

    if (applied.plain.length + applied.versioned.length === 0 && remainingObjects.length === 0) {
      await this.storage.asSuperUser().findBucket(Bucket)
    }

    const remainingNames = new Set<string>()
    for (const object of remainingObjects) {
      remainingNames.add(object.name)
    }

    const deleted: {
      Key: string
      VersionId?: string
      DeleteMarker?: boolean
      DeleteMarkerVersionId?: string
    }[] = []
    const errors: { Key?: string; VersionId?: string; Code: string; Message: string }[] = []
    const accessDenied = (Key: string | undefined, VersionId?: string) =>
      errors.push({ Key, VersionId, Code: 'AccessDenied', Message: 'Access Denied' })

    for (const { object, ref, invalidVersion } of targets) {
      const Key = object.Key

      if (Key === undefined) {
        accessDenied(Key)
        continue
      }

      if (invalidVersion) {
        errors.push({
          Key,
          VersionId: object.VersionId,
          Code: ErrorCode.NoSuchVersion,
          Message: 'The specified version does not exist.',
        })
        continue
      }

      if (!ref) {
        const row = plainDeleted.get(Key)
        if (row?.is_delete_marker) {
          deleted.push({ Key, DeleteMarker: true, DeleteMarkerVersionId: toS3VersionId(row) })
        } else if (row || !remainingNames.has(Key)) {
          deleted.push({ Key })
        } else {
          accessDenied(Key)
        }
        continue
      }

      const VersionId = object.VersionId
      const target = resolveVersion(Key, ref)
      if (!target?.version) {
        deleted.push({ Key, VersionId })
        continue
      }

      const row = versionsDeleted.get(versionKey(target.name, target.version))
      if (!row) {
        accessDenied(Key, VersionId)
      } else if (row.is_delete_marker) {
        deleted.push({ Key, VersionId, DeleteMarker: true, DeleteMarkerVersionId: VersionId })
      } else {
        deleted.push({ Key, VersionId })
      }
    }

    return {
      responseBody: {
        DeleteResult: {
          Deleted: Delete.Quiet ? [] : deleted,
          Error: errors,
        },
      },
    }
  }

  /**
   * Creates a copy of an object that is already stored in Amazon S3.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
   *
   * @param command
   */
  async copyObject(command: CopyObjectCommandInput) {
    const { Bucket, Key, CopySource, MetadataDirective = 'COPY' } = command

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (!Key) {
      throw ERRORS.MissingParameter('Key')
    }

    if (!CopySource) {
      throw ERRORS.MissingParameter('CopySource')
    }

    const {
      bucket: sourceBucket,
      key: sourceKey,
      versionId: sourceVersionId,
    } = parseCopySource(CopySource)

    if (!sourceBucket) {
      throw ERRORS.InvalidBucketName('')
    }

    if (!sourceKey) {
      throw ERRORS.MissingParameter('CopySource')
    }

    const sourceRef = parseCopySourceVersionId(sourceVersionId)
    const sourceVersion = sourceRef
      ? await this.findCopySourceVersion(sourceBucket, sourceKey, sourceRef, 'version')
      : undefined

    try {
      const copyResult = await this.storage.from(sourceBucket).copyObject({
        sourceKey,
        sourceVersionId: sourceVersion?.version,
        destinationBucket: Bucket,
        destinationKey: Key,
        owner: this.owner,
        upsert: true,
        conditions: {
          ifMatch: command.CopySourceIfMatch,
          ifNoneMatch: command.CopySourceIfNoneMatch,
          ifModifiedSince: command.CopySourceIfModifiedSince,
          ifUnmodifiedSince: command.CopySourceIfUnmodifiedSince,
        },
        metadata: {
          cacheControl: command.CacheControl,
          contentEncoding: command.ContentEncoding,
          mimetype: command.ContentType,
        },
        userMetadata: command.Metadata,
        copyMetadata: MetadataDirective === 'COPY',
        uploadType: 's3',
      })

      return {
        headers: {
          ...copySourceVersionHeader(copyResult.sourceObject, sourceVersionId),
          ...versionIdHeader(copyResult.destObject),
        },
        responseBody: {
          CopyObjectResult: {
            ETag: copyResult.eTag,
            LastModified: copyResult.lastModified?.toISOString(),
          },
        },
      }
    } catch (e) {
      if (!sourceRef) {
        await this.addCurrentDeleteMarkerHeaders(e, sourceBucket, sourceKey)
      }
      throw e
    }
  }

  /**
   * Lists the parts that have been uploaded for a specific multipart upload.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
   *
   * @param command
   */
  async listParts(command: ListPartsCommandInput) {
    if (!command.UploadId) {
      throw ERRORS.MissingParameter('UploadId')
    }

    const multipart = await this.storage.db
      .asSuperUser()
      .findMultipartUpload(command.UploadId, 'id,bucket_id,key')

    assertMultipartUploadIdentity(multipart, command.Bucket, command.Key, command.UploadId)

    const maxParts = Math.min(command.MaxParts || 1000, 1000)

    const result = await this.storage.db.listParts(command.UploadId, {
      afterPart: command.PartNumberMarker,
      maxParts: maxParts + 1,
    })

    const isTruncated = result.length > maxParts
    const resultCount = isTruncated ? maxParts : result.length
    const nextPartNumberMarker = isTruncated ? result[resultCount - 1].part_number : undefined

    const parts: {
      PartNumber: number
      Size: number
      LastModified: string | undefined
      ETag: string | undefined
    }[] = []
    for (let index = 0; index < resultCount; index++) {
      const part = result[index]
      parts.push({
        PartNumber: part.part_number,
        Size: part.size ?? 0,
        LastModified: part.created_at ? new Date(part.created_at).toISOString() : undefined,
        ETag: part.etag,
      })
    }

    return {
      responseBody: {
        ListPartsResult: {
          Bucket: command.Bucket,
          Key: command.Key,
          UploadId: command.UploadId,
          PartNumberMarker: command.PartNumberMarker,
          NextPartNumberMarker: nextPartNumberMarker,
          MaxParts: maxParts,
          IsTruncated: isTruncated,
          Part: parts,
        },
      },
    }
  }

  /**
   * Uploads a part by copying data from an existing object as data source. To specify the data source, you add the request header x-amz-copy-source in your request. To specify a byte range, you add the request header x-amz-copy-source-range in your request.
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
   *
   * @param command UploadPartCopyCommandInput
   */
  async uploadPartCopy(command: UploadPartCopyCommandInput) {
    const { Bucket, Key, UploadId, PartNumber, CopySource, CopySourceRange } = command

    if (!UploadId) {
      throw ERRORS.MissingParameter('UploadId')
    }

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (!Key) {
      throw ERRORS.MissingParameter('Key')
    }

    if (!PartNumber) {
      throw ERRORS.MissingParameter('PartNumber')
    }

    if (!CopySource) {
      throw ERRORS.MissingParameter('CopySource')
    }

    const {
      bucket: sourceBucketName,
      key: sourceKey,
      versionId: sourceVersionId,
    } = parseCopySource(CopySource)

    if (!sourceBucketName) {
      throw ERRORS.NoSuchBucket('')
    }

    if (!sourceKey) {
      throw ERRORS.NoSuchKey('')
    }

    const multipartData = await this.storage.db
      .asSuperUser()
      .findMultipartUpload(UploadId, 'version,user_metadata,metadata,bucket_id,key')

    assertMultipartUploadIdentity(multipartData, Bucket, Key, UploadId)

    const sourceRef = parseCopySourceVersionId(sourceVersionId)
    const copySourceColumns = 'id,name,version,metadata,is_versioned'
    const copySource = sourceRef
      ? await this.findCopySourceVersion(sourceBucketName, sourceKey, sourceRef, copySourceColumns)
      : await this.findCurrentCopySource(sourceBucketName, sourceKey, copySourceColumns)

    const sourceSize = Number(copySource.metadata?.size ?? 0)
    let copySize = sourceSize
    let rangeBytes: { fromByte: number; toByte: number } | undefined = undefined

    if (CopySourceRange) {
      const range = parseCopySourceRangeHeader(CopySourceRange, sourceSize)
      rangeBytes = range
      copySize = range.size
    }

    const uploader = new Uploader(this.storage.backend, this.storage.db, this.storage.location)

    const [destinationBucket] = await this.storage.db.asSuperUser().withTransaction(async (db) => {
      return Promise.all([
        db.findBucketById(Bucket, 'file_size_limit'),
        db.findBucketById(sourceBucketName, 'id'),
      ])
    })
    const maxFileSize = await getFileSizeLimit(
      this.storage.db.tenantId,
      destinationBucket?.file_size_limit
    )

    await uploader.canUpload({
      bucketId: Bucket,
      objectName: Key,
      owner: this.owner,
      isUpsert: true,
      userMetadata: multipartData.user_metadata || undefined,
      metadata: multipartData.metadata || undefined,
    })

    const multipart = await this.shouldAllowPartUpload(UploadId, Number(copySize), maxFileSize)

    const uploadPart = await this.storage.backend.uploadPartCopy(
      storageS3Bucket,
      this.storage.location.getKeyLocation({
        bucketId: Bucket,
        objectName: Key,
        tenantId: this.tenantId,
      }),
      multipart.version,
      UploadId,
      PartNumber,
      this.storage.location.getKeyLocation({
        bucketId: sourceBucketName,
        objectName: copySource.name,
        tenantId: this.tenantId,
      }),
      copySource.version,
      rangeBytes
    )

    await this.storage.db.asSuperUser().insertUploadPart({
      upload_id: UploadId,
      version: multipart.version,
      part_number: PartNumber,
      size: Number(copySize),
      etag: uploadPart.eTag || '',
      key: Key as string,
      bucket_id: Bucket,
      owner_id: this.owner,
    })

    return {
      headers: copySourceVersionHeader(copySource, sourceVersionId),
      responseBody: {
        CopyPartResult: {
          ETag: uploadPart.eTag || '',
          LastModified: uploadPart.lastModified ? uploadPart.lastModified.toISOString() : undefined,
        },
      },
    }
  }

  /**
   * The row of the source version a copy names. S3 refuses to copy a delete
   * marker addressed by version id, and reports a version that does not
   * exist as NoSuchVersion.
   */
  protected async findCopySourceVersion(
    bucket: string,
    key: string,
    ref: S3VersionRef,
    columns: string
  ) {
    const lookup = versionLookup(ref)
    const source = await this.storage
      .from(bucket)
      .findObject(
        key,
        `${columns},is_delete_marker`,
        { ...lookup.filters, excludeDeleteMarkers: false, dontErrorOnEmpty: true },
        lookup.version
      )

    if (!source) {
      throw ERRORS.NoSuchVersion(key)
    }
    if (source.is_delete_marker) {
      throw ERRORS.InvalidRequest(
        'The source of a copy request may not specifically refer to a delete marker by version id.'
      )
    }
    return source
  }

  /** The current row of a copy source, which S3 reports missing when it is a delete marker. */
  protected async findCurrentCopySource(bucket: string, key: string, columns: string) {
    const source = await this.storage.from(bucket).findObject(key, `${columns},is_delete_marker`, {
      excludeDeleteMarkers: false,
      dontErrorOnEmpty: true,
    })

    if (!source) {
      throw ERRORS.NoSuchKey(key)
    }
    if (source.is_delete_marker) {
      throw ERRORS.NoSuchKey(key).withResponseHeaders(deleteMarkerHeaders(source))
    }
    return source
  }

  /**
   * Adds the delete marker headers S3 sends with a copy's NoSuchKey when the
   * source's current row is a delete marker. Only the failure path pays for
   * the extra lookup, and a failed lookup leaves the error as it is.
   */
  protected async addCurrentDeleteMarkerHeaders(error: unknown, bucket: string, key: string) {
    if (!isStorageError(ErrorCode.NoSuchKey, error)) {
      return
    }
    try {
      const current = await this.storage
        .from(bucket)
        .findObject(key, 'version,is_versioned,is_delete_marker', {
          excludeDeleteMarkers: false,
          dontErrorOnEmpty: true,
        })
      if (current?.is_delete_marker) {
        error.withResponseHeaders(deleteMarkerHeaders(current))
      }
    } catch {
      // Report the copy's own error.
    }
  }

  parseMetadataHeaders(headers: Record<string, unknown>): Record<string, string> | undefined {
    let metadata: Record<string, string> | undefined
    const metadataPrefix = 'x-amz-meta-'

    for (const key in headers) {
      if (!key.startsWith(metadataPrefix)) {
        continue
      }

      if (!Object.prototype.hasOwnProperty.call(headers, key)) {
        continue
      }

      const value = headers[key]
      if (typeof value !== 'string') {
        continue
      }

      if (!metadata) {
        metadata = {}
      }

      metadata[key.slice(metadataPrefix.length)] = value
    }

    return metadata
  }

  protected uploadSignature({ in_progress_size }: { in_progress_size: number }) {
    return `${encrypt('progress:' + in_progress_size.toString())}`
  }

  protected decryptUploadSignature(signature: string) {
    const originalSignature = decrypt(signature)
    const [, value] = originalSignature.split(':')

    return {
      progress: parseInt(value, 10),
    }
  }

  protected async shouldAllowPartUpload(
    uploadId: string,
    contentLength: number,
    maxFileSize: number
  ) {
    return this.storage.db.asSuperUser().withTransaction(async (db) => {
      const multipart = await db.findMultipartUpload(
        uploadId,
        'in_progress_size,version,upload_signature,user_metadata,metadata',
        {
          forUpdate: true,
        }
      )

      const { progress } = this.decryptUploadSignature(multipart.upload_signature)

      if (progress !== multipart.in_progress_size) {
        throw ERRORS.InvalidUploadSignature()
      }

      const currentProgress = multipart.in_progress_size + contentLength

      if (currentProgress > maxFileSize) {
        throw ERRORS.EntityTooLarge()
      }

      const signature = this.uploadSignature({ in_progress_size: currentProgress })
      await db.updateMultipartUploadProgress(uploadId, currentProgress, signature)
      return multipart
    })
  }
}

function toAwsMetadataHeaders(records: Record<string, unknown>) {
  const metadataHeaders: Record<string, unknown> = {}
  let missingCount = 0

  for (const key in records) {
    if (!Object.prototype.hasOwnProperty.call(records, key)) {
      continue
    }

    if (key.length === 0) {
      missingCount++
      continue
    }

    const value = records[key]
    const headerName = 'x-amz-meta-' + key.toLowerCase()
    if (typeof value === 'string' && isUSASCII(value)) {
      const previousValue = metadataHeaders[headerName]
      const headerValue = previousValue === undefined ? value : `${previousValue},${value}`
      if (isValidHeader(headerName, headerValue)) {
        metadataHeaders[headerName] = headerValue
        continue
      }
    }
    missingCount++
  }

  if (missingCount > 0) {
    metadataHeaders['x-amz-missing-meta'] = missingCount
  }

  return metadataHeaders
}

function isUSASCII(str: string): boolean {
  for (let i = 0; i < str.length; i++) {
    if (str.charCodeAt(i) > 127) {
      return false
    }
  }
  return true
}
