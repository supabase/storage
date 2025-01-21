import { Storage } from '../../storage'
import { getConfig } from '../../../config'
import { Uploader, validateMimeType } from '../../uploader'
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
  ListMultipartUploadsCommandInput,
  ListObjectsCommandInput,
  ListObjectsV2CommandInput,
  ListObjectsV2Output,
  ListPartsCommandInput,
  PutObjectCommandInput,
  UploadPartCommandInput,
  UploadPartCopyCommandInput,
} from '@aws-sdk/client-s3'
import { PassThrough, Readable } from 'stream'
import stream from 'stream/promises'
import { getFileSizeLimit, mustBeValidBucketName, mustBeValidKey } from '../../limits'
import { ERRORS } from '@internal/errors'
import { S3MultipartUpload, Obj } from '../../schemas'
import { decrypt, encrypt } from '@internal/auth'
import { ByteLimitTransformStream } from './byte-limit-stream'
import { logger, logSchema } from '@internal/monitoring'

const { storageS3Region, storageS3Bucket } = getConfig()

export class S3ProtocolHandler {
  constructor(
    protected readonly storage: Storage,
    protected readonly tenantId: string,
    protected readonly owner?: string
  ) {}

  /**
   * Returns the versioning state of a bucket.
   * default: versioning is suspended
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html
   */
  async getBucketVersioning() {
    return {
      responseBody: {
        VersioningConfiguration: {
          Status: 'Suspended',
          MfaDelete: 'Disabled',
        },
      },
    }
  }

  /**
   * Returns the Region the bucket resides in
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
   */
  async getBucketLocation() {
    return {
      responseBody: {
        LocationConstraint: {
          LocationConstraint: storageS3Region,
        },
      },
    }
  }

  /**
   * Returns a list of all buckets owned by the authenticated sender of the request
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
   */
  async listBuckets() {
    const buckets = await this.storage.listBuckets('name,created_at')

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
    await this.storage.deleteBucket(name)

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
      ContinuationToken: command.Marker,
    })

    return {
      responseBody: {
        ListBucketResult: {
          Name: list.responseBody.ListBucketResult.Name,
          Prefix: list.responseBody.ListBucketResult.Prefix,
          Marker: list.responseBody.ListBucketResult.ContinuationToken,
          MaxKeys: list.responseBody.ListBucketResult.MaxKeys,
          IsTruncated: list.responseBody.ListBucketResult.IsTruncated,
          Contents: list.responseBody.ListBucketResult.Contents,
          CommonPrefixes: list.responseBody.ListBucketResult.CommonPrefixes,
          EncodingType: list.responseBody.ListBucketResult.EncodingType,
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
  async listObjectsV2(command: ListObjectsV2CommandInput) {
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

    const limit = Math.min(maxKeys || 1000, 1000)

    const objects = await this.storage.from(bucket).listObjectsV2({
      prefix,
      delimiter: delimiter,
      maxKeys: limit + 1,
      nextToken: continuationToken ? decodeContinuationToken(continuationToken) : undefined,
      startAfter,
    })

    let results = objects
    let prevPrefix = ''

    if (delimiter) {
      const delimitedResults: Obj[] = []
      for (const object of objects) {
        let idx = object.name.replace(prefix, '').indexOf(delimiter)

        if (idx >= 0) {
          idx = prefix.length + idx + delimiter.length
          const currPrefix = object.name.substring(0, idx)
          if (currPrefix === prevPrefix) {
            continue
          }
          prevPrefix = currPrefix
          delimitedResults.push({
            id: null,
            name: command.EncodingType === 'url' ? encodeURIComponent(currPrefix) : currPrefix,
            bucket_id: bucket,
            owner: '',
            metadata: null,
            created_at: '',
            updated_at: '',
            version: '',
          })
          continue
        }

        delimitedResults.push(object)
      }
      results = delimitedResults
    }

    let isTruncated = false

    if (results.length > limit) {
      results = results.slice(0, limit)
      isTruncated = true
    }

    const commonPrefixes = results
      .filter((e) => e.id === null)
      .map((object) => {
        return {
          Prefix: object.name,
        }
      })

    const contents =
      results
        .filter((o) => o.id)
        .map((o) => ({
          Key: command.EncodingType === 'url' ? encodeURIComponent(o.name) : o.name,
          LastModified: (o.updated_at ? new Date(o.updated_at).toISOString() : undefined) as
            | Date
            | undefined,
          ETag: o.metadata?.eTag as string,
          Size: o.metadata?.size as number,
          StorageClass: 'STANDARD' as const,
        })) || []

    const nextContinuationToken = isTruncated
      ? encodeContinuationToken(results[results.length - 1].name)
      : undefined

    const response: { ListBucketResult: ListObjectsV2Output } = {
      ListBucketResult: {
        Name: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
        Contents: contents,
        IsTruncated: isTruncated,
        MaxKeys: limit,
        Delimiter: delimiter,
        EncodingType: encodingType,
        KeyCount: results.length,
        CommonPrefixes: commonPrefixes,
      },
    }

    if (nextContinuationToken) {
      response.ListBucketResult.NextContinuationToken = nextContinuationToken
    }

    return {
      responseBody: response,
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

    await this.storage.asSuperUser().findBucket(command.Bucket)

    const keyContinuationToken = command.KeyMarker
    const uploadContinuationToken = command.UploadIdMarker

    const encodingType = command.EncodingType
    const delimiter = command.Delimiter
    const prefix = command.Prefix || ''
    const maxKeys = command.MaxUploads
    const bucket = command.Bucket

    const limit = maxKeys || 200

    const multipartUploads = await this.storage.db.listMultipartUploads(bucket, {
      prefix,
      deltimeter: delimiter,
      maxKeys: limit + 1,
      nextUploadKeyToken: keyContinuationToken
        ? decodeContinuationToken(keyContinuationToken)
        : undefined,
      nextUploadToken: uploadContinuationToken
        ? decodeContinuationToken(uploadContinuationToken)
        : undefined,
    })

    let results: Partial<S3MultipartUpload & { isFolder: boolean }>[] = multipartUploads
    let prevPrefix = ''

    if (delimiter) {
      const delimitedResults: Partial<S3MultipartUpload & { isFolder: boolean }>[] = []
      for (const object of multipartUploads) {
        let idx = object.key.replace(prefix, '').indexOf(delimiter)

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
            key: command.EncodingType === 'url' ? encodeURIComponent(currPrefix) : currPrefix,
            bucket_id: bucket,
          })
          continue
        }

        delimitedResults.push(object)
      }
      results = delimitedResults
    }

    let isTruncated = false

    if (results.length > limit) {
      results = results.slice(0, limit)
      isTruncated = true
    }

    const commonPrefixes = results
      .filter((e) => e.isFolder)
      .map((object) => {
        return {
          Prefix: object.key,
        }
      })

    const uploads =
      results
        .filter((o) => !o.isFolder)
        .map((o) => ({
          Key: command.EncodingType === 'url' && o.key ? encodeURIComponent(o.key) : o.key,
          Initiated: o.created_at ? new Date(o.created_at).toISOString() : undefined,
          UploadId: o.id,
          StorageClass: 'STANDARD',
        })) || []

    let keyNextContinuationToken: string | undefined
    let uploadNextContinuationToken: string | undefined

    if (isTruncated) {
      const lastItem = results[results.length - 1]
      keyNextContinuationToken = encodeContinuationToken(lastItem.key!)
      uploadNextContinuationToken = encodeContinuationToken(lastItem.id!)
    }

    const response = {
      ListMultipartUploadsResult: {
        Name: bucket,
        Prefix: prefix,
        KeyMarker: keyContinuationToken,
        UploadIdMarker: uploadContinuationToken,
        NextKeyMarker: keyNextContinuationToken,
        NextUploadIdMarker: uploadNextContinuationToken,
        Upload: uploads,
        IsTruncated: isTruncated,
        MaxUploads: limit,
        Delimiter: delimiter,
        EncodingType: encodingType,
        KeyCount: results.length,
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
    const uploader = new Uploader(this.storage.backend, this.storage.db)
    const { Bucket, Key } = command

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
    })

    const uploadId = await this.storage.backend.createMultiPartUpload(
      storageS3Bucket,
      `${this.tenantId}/${command.Bucket}/${command.Key}`,
      version,
      command.ContentType || '',
      command.CacheControl || ''
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
        command.Metadata
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
    const uploader = new Uploader(this.storage.backend, this.storage.db)
    const { Bucket, Key, UploadId } = command

    if (!UploadId) {
      throw ERRORS.InvalidUploadId()
    }

    await uploader.canUpload({
      bucketId: Bucket as string,
      objectName: Key as string,
      isUpsert: true,
      owner: this.owner,
    })

    const multiPartUpload = await this.storage.db
      .asSuperUser()
      .findMultipartUpload(UploadId, 'id,version,user_metadata')

    const parts = command.MultipartUpload?.Parts || []

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
      `${this.tenantId}/${Bucket}/${Key}`,
      UploadId as string,
      multiPartUpload.version,
      parts
    )

    const metadata = await this.storage.backend.headObject(
      storageS3Bucket,
      `${this.tenantId}/${Bucket}/${Key}`,
      resp.version
    )

    await uploader.completeUpload({
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
      responseBody: {
        CompleteMultipartUploadResult: {
          Location: `${Bucket}/${Key}`,
          Bucket: Bucket,
          Key: Key,
          ChecksumCRC32: resp.ChecksumCRC32,
          ChecksumCRC32C: resp.ChecksumCRC32,
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
  async uploadPart(command: UploadPartCommandInput, signal?: AbortSignal) {
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

    const bucket = await this.storage.asSuperUser().findBucket(Bucket, 'file_size_limit')
    const maxFileSize = await getFileSizeLimit(this.storage.db.tenantId, bucket?.file_size_limit)

    const uploader = new Uploader(this.storage.backend, this.storage.db)
    await uploader.canUpload({
      bucketId: Bucket as string,
      objectName: Key as string,
      owner: this.owner,
      isUpsert: true,
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
        new ByteLimitTransformStream(ContentLength),
        async (stream) => {
          return this.storage.backend.uploadPart(
            storageS3Bucket,
            `${this.tenantId}/${Bucket}/${Key}`,
            multipart.version,
            UploadId,
            PartNumber || 0,
            stream as Readable,
            ContentLength,
            signal
          )
        }
      )

      await this.storage.db.asSuperUser().insertUploadPart({
        upload_id: UploadId,
        version: multipart.version,
        part_number: PartNumber || 0,
        etag: uploadPart.ETag || '',
        key: Key as string,
        bucket_id: Bucket,
        owner_id: this.owner,
      })

      return {
        headers: {
          etag: uploadPart.ETag || '',
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
    options: { signal?: AbortSignal; isTruncated: () => boolean }
  ) {
    const uploader = new Uploader(this.storage.backend, this.storage.db)

    mustBeValidBucketName(command.Bucket)
    mustBeValidKey(command.Key)

    const upload = await uploader.upload({
      bucketId: command.Bucket as string,
      file: {
        body: command.Body as Readable,
        cacheControl: command.CacheControl!,
        mimeType: command.ContentType!,
        isTruncated: options.isTruncated,
        userMetadata: command.Metadata,
      },
      objectName: command.Key as string,
      owner: this.owner,
      isUpsert: true,
      uploadType: 's3',
      signal: options.signal,
    })

    return {
      headers: {
        etag: upload.metadata.eTag,
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
      .findMultipartUpload(UploadId, 'id,version')

    const uploader = new Uploader(this.storage.backend, this.storage.db)
    await uploader.canUpload({
      bucketId: Bucket,
      objectName: Key,
      owner: this.owner,
      isUpsert: true,
    })

    await this.storage.backend.abortMultipartUpload(
      storageS3Bucket,
      `${this.tenantId}/${Bucket}/${Key}`,
      UploadId,
      multipart.version
    )

    await this.storage.db.asSuperUser().deleteMultipartUpload(UploadId)

    return {}
  }

  /**
   * The HEAD operation retrieves metadata from an object without returning the object itself. This operation is useful if you're interested only in an object's metadata.
   *
   * Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
   *
   * @param command
   */
  async headObject(command: HeadObjectCommandInput) {
    const { Bucket, Key } = command

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (!Key) {
      throw ERRORS.MissingParameter('Bucket')
    }

    const object = await this.storage
      .from(Bucket)
      .findObject(Key, 'metadata,user_metadata,created_at,updated_at')

    if (!object) {
      throw ERRORS.NoSuchKey(Key)
    }

    let metadataHeaders: Record<string, any> = {}

    if (object.user_metadata) {
      metadataHeaders = toAwsMeatadataHeaders(object.user_metadata)
    }

    return {
      headers: {
        'created-at': (object.created_at as string) || '',
        'cache-control': (object.metadata?.cacheControl as string) || '',
        expires: (object.metadata?.expires as string) || '',
        'content-length': (object.metadata?.size as string) || '',
        'content-type': (object.metadata?.mimetype as string) || '',
        etag: (object.metadata?.eTag as string) || '',
        'last-modified': object.updated_at ? new Date(object.updated_at).toUTCString() || '' : '',
        ...metadataHeaders,
      },
    }
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
  async getObject(command: GetObjectCommandInput, options?: { signal?: AbortSignal }) {
    const bucket = command.Bucket as string
    const key = command.Key as string

    const object = await this.storage.from(bucket).findObject(key, 'version,user_metadata')
    const response = await this.storage.backend.getObject(
      storageS3Bucket,
      `${this.tenantId}/${bucket}/${key}`,
      object.version,
      {
        ifModifiedSince: command.IfModifiedSince?.toISOString(),
        ifNoneMatch: command.IfNoneMatch,
        range: command.Range,
      },
      options?.signal
    )

    let metadataHeaders: Record<string, any> = {}

    if (object.user_metadata) {
      metadataHeaders = toAwsMeatadataHeaders(object.user_metadata)
    }

    return {
      headers: {
        'cache-control': response.metadata.cacheControl,
        'content-length': response.metadata.contentLength?.toString() || '0',
        'content-range': response.metadata.contentRange?.toString() || '',
        'content-type': response.metadata.mimetype,
        etag: response.metadata.eTag,
        'last-modified': response.metadata.lastModified?.toUTCString() || '',
        ...metadataHeaders,
      },
      responseBody: response.body,
      statusCode: command.Range ? 206 : 200,
    }
  }

  /**
   * Removes an object from a bucket.
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

    await this.storage.from(Bucket).deleteObject(Key)

    return {}
  }

  /**
   * This operation enables you to delete multiple objects from a bucket using a single HTTP request.
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
      return {}
    }

    const deletedResult = await this.storage
      .from(Bucket)
      .deleteObjects(Delete.Objects.map((o) => o.Key || ''))

    const deleted = Delete.Objects.filter((o) => deletedResult.find((d) => d.name === o.Key)).map(
      (o) => ({ Key: o.Key })
    )

    const errors = Delete.Objects.filter((o) => !deletedResult.find((d) => d.name === o.Key)).map(
      (o) => ({
        Key: o.Key,
        Code: 'AccessDenied',
        Message: "You do not have permission to delete this object or the object doesn't exists",
      })
    )

    return {
      responseBody: {
        DeleteResult: {
          Deleted: deleted,
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
    const { Bucket, Key, CopySource } = command

    if (!Bucket) {
      throw ERRORS.MissingParameter('Bucket')
    }

    if (!Key) {
      throw ERRORS.MissingParameter('Key')
    }

    if (!CopySource) {
      throw ERRORS.MissingParameter('CopySource')
    }

    const sourceBucket = (
      CopySource.startsWith('/') ? CopySource.replace('/', '').split('/') : CopySource.split('/')
    ).shift()

    const sourceKey = (CopySource.startsWith('/') ? CopySource.replace('/', '') : CopySource)
      .split('/')
      .slice(1)
      .join('/')

    if (!sourceBucket) {
      throw ERRORS.InvalidBucketName('')
    }

    if (!sourceKey) {
      throw ERRORS.MissingParameter('CopySource')
    }

    if (!command.MetadataDirective) {
      // default metadata directive is copy
      command.MetadataDirective = 'COPY'
    }

    const copyResult = await this.storage.from(sourceBucket).copyObject({
      sourceKey,
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
        mimetype: command.ContentType,
      },
      userMetadata: command.Metadata,
      copyMetadata: command.MetadataDirective === 'COPY',
    })

    return {
      responseBody: {
        CopyObjectResult: {
          ETag: copyResult.eTag,
          LastModified: copyResult.lastModified?.toISOString(),
        },
      },
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

    // check if multipart exists
    await this.storage.db.asSuperUser().findMultipartUpload(command.UploadId, 'id')

    const maxParts = Math.min(command.MaxParts || 1000, 1000)

    let result = await this.storage.db.listParts(command.UploadId, {
      afterPart: command.PartNumberMarker,
      maxParts: maxParts + 1,
    })

    const isTruncated = result.length > maxParts
    if (isTruncated) {
      result = result.slice(0, maxParts)
    }
    const nextPartNumberMarker = isTruncated ? result[result.length - 1].part_number : undefined

    const parts = result.map((part) => ({
      PartNumber: part.part_number,
      LastModified: part.created_at ? new Date(part.created_at).toISOString() : undefined,
      ETag: part.etag,
    }))

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

    if (!CopySourceRange) {
      throw ERRORS.MissingParameter('CopySourceRange')
    }

    const sourceBucketName = (
      CopySource.startsWith('/') ? CopySource.replace('/', '').split('/') : CopySource.split('/')
    ).shift()

    const sourceKey = (CopySource.startsWith('/') ? CopySource.replace('/', '') : CopySource)
      .split('/')
      .slice(1)
      .join('/')

    if (!sourceBucketName) {
      throw ERRORS.NoSuchBucket('')
    }

    if (!sourceKey) {
      throw ERRORS.NoSuchKey('')
    }

    // Check if copy source exists
    const copySource = await this.storage.db.findObject(
      sourceBucketName,
      sourceKey,
      'id,name,version,metadata'
    )

    let copySize = copySource.metadata?.size || 0
    let rangeBytes: { fromByte: number; toByte: number } | undefined = undefined

    if (CopySourceRange) {
      const bytes = CopySourceRange.split('=')[1].split('-')

      if (bytes.length !== 2) {
        throw ERRORS.InvalidRange()
      }

      const fromByte = Number(bytes[0])
      const toByte = Number(bytes[1])

      if (isNaN(fromByte) || isNaN(toByte)) {
        throw ERRORS.InvalidRange()
      }

      rangeBytes = { fromByte, toByte }
      copySize = toByte - fromByte
    }

    const uploader = new Uploader(this.storage.backend, this.storage.db)

    await uploader.canUpload({
      bucketId: Bucket,
      objectName: Key,
      owner: this.owner,
      isUpsert: true,
    })

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

    const multipart = await this.shouldAllowPartUpload(UploadId, Number(copySize), maxFileSize)

    const uploadPart = await this.storage.backend.uploadPartCopy(
      storageS3Bucket,
      `${this.tenantId}/${Bucket}/${Key}`,
      multipart.version,
      UploadId,
      PartNumber,
      `${this.tenantId}/${sourceBucketName}/${copySource.name}`,
      copySource.version,
      rangeBytes
    )

    await this.storage.db.asSuperUser().insertUploadPart({
      upload_id: UploadId,
      version: multipart.version,
      part_number: PartNumber,
      etag: uploadPart.eTag || '',
      key: Key as string,
      bucket_id: Bucket,
      owner_id: this.owner,
    })

    return {
      responseBody: {
        ETag: uploadPart.eTag || '',
        LastModified: uploadPart.lastModified ? uploadPart.lastModified.toISOString() : undefined,
      },
    }
  }

  parseMetadataHeaders(headers: Record<string, any>) {
    let metadata: Record<string, any> | undefined = undefined

    Object.keys(headers)
      .filter((key) => key.startsWith('x-amz-meta-'))
      .forEach((key) => {
        if (!metadata) metadata = {}
        metadata[key.replace('x-amz-meta-', '')] = headers[key]
      })

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
        'in_progress_size,version,upload_signature',
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

function toAwsMeatadataHeaders(records: Record<string, any>) {
  const metadataHeaders: Record<string, any> = {}
  let missingCount = 0

  if (records) {
    Object.keys(records).forEach((key) => {
      const value = records[key]
      if (value && isUSASCII(value)) {
        metadataHeaders['x-amz-meta-' + key.toLowerCase()] = value
      } else {
        missingCount++
      }
    })
  }

  if (missingCount) {
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

function encodeContinuationToken(name: string) {
  return Buffer.from(`l:${name}`).toString('base64')
}

function decodeContinuationToken(token: string) {
  const decoded = Buffer.from(token, 'base64').toString().split(':')

  if (decoded.length === 0) {
    throw new Error('Invalid continuation token')
  }

  return decoded[1]
}
