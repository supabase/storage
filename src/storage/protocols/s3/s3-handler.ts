import { Storage } from '../../storage'
import { getConfig } from '../../../config'
import { getMaxFileSizeLimit, Uploader } from '../../uploader'
import {
  AbortMultipartUploadCommandInput,
  CompleteMultipartUploadCommandInput,
  CopyObjectCommandInput,
  CreateMultipartUploadCommandInput,
  DeleteObjectCommandInput,
  DeleteObjectsCommandInput,
  GetObjectCommandInput,
  HeadObjectCommandInput,
  ListMultipartUploadsCommandInput,
  ListObjectsV2CommandInput,
  ListPartsCommandInput,
  PutObjectCommandInput,
  UploadPartCommandInput,
} from '@aws-sdk/client-s3'
import { PassThrough, Readable } from 'stream'
import stream from 'stream/promises'
import { mustBeValidBucketName, mustBeValidKey } from '../../limits'
import { ERRORS } from '../../errors'
import { S3MultipartUpload, Obj } from '../../schemas'
import { decrypt, encrypt } from '../../../auth'
import { ByteLimitTransformStream } from './byte-limit-stream'
import { randomUUID } from 'crypto'

const { storageS3Region, storageS3Bucket } = getConfig()

export class S3ProtocolHandler {
  constructor(protected readonly storage: Storage, protected readonly tenantId: string) {}

  /**
   * Get the versioning configuration of a bucket
   * default: versioning is suspended
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
   * Get the location of a bucket
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
   * List all buckets
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
   * Create a new bucket
   * @param Bucket
   * @param isPublic
   */
  async createBucket(Bucket: string, isPublic: boolean) {
    mustBeValidBucketName(Bucket || '')

    await this.storage.createBucket({
      name: Bucket,
      id: Bucket,
      public: isPublic,
    })

    return {
      headers: {
        Location: `/${Bucket}`,
      },
    }
  }

  /**
   * Delete a bucket
   * @param name
   */
  async deleteBucket(name: string) {
    await this.storage.deleteBucket(name)

    return {
      statusCode: 204,
    }
  }

  /**
   * Head bucket
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
   * List objects in a bucket, implements the ListObjectsV2Command
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
    const bucket = command.Bucket!

    const limit = maxKeys || 200

    const objects = await this.storage.from(bucket).listObjectsV2({
      prefix,
      deltimeter: delimiter,
      maxKeys: limit + 1,
      nextToken: continuationToken ? decodeContinuationToken(continuationToken) : undefined,
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
      .map((object) => {
        if (object.id === null) {
          return {
            Prefix: command.EncodingType === 'url' ? encodeURIComponent(object.name) : object.name,
          }
        }
      })
      .filter((e) => e)

    const contents =
      results
        .filter((o) => o.id)
        .map((o) => ({
          Key: command.EncodingType === 'url' ? encodeURIComponent(o.name) : o.name,
          LastModified: o.updated_at ? new Date(o.updated_at).toISOString() : undefined,
          ETag: o.metadata?.eTag,
          Size: o.metadata?.size,
          StorageClass: 'STANDARD',
        })) || []

    const nextContinuationToken = isTruncated
      ? encodeContinuationToken(results[results.length - 1].name)
      : undefined

    const response = {
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
        NextContinuationToken: nextContinuationToken,
      },
    }

    return {
      responseBody: response,
    }
  }

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
    const bucket = command.Bucket!

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
      .map((object) => {
        if (object.isFolder) {
          return {
            Prefix:
              command.EncodingType === 'url' && object.key
                ? encodeURIComponent(object.key)
                : object.key,
          }
        }
      })
      .filter((e) => e)

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
   * Create a multipart upload
   * @param command
   */
  async createMultiPartUpload(command: CreateMultipartUploadCommandInput) {
    const uploader = new Uploader(this.storage.backend, this.storage.db)
    const { Bucket, Key } = command

    mustBeValidBucketName(Bucket)
    mustBeValidKey(Key)

    await this.storage.asSuperUser().findBucket(Bucket)

    // Create Multi Part Upload
    const version = await uploader.prepareUpload({
      bucketId: command.Bucket as string,
      objectName: command.Key as string,
      isUpsert: true,
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

    const signature = this.uploadSignature({ in_progress_size: BigInt(0) })
    await this.storage.db
      .asSuperUser()
      .createMultipartUpload(uploadId, Bucket, Key, version, signature)

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
   * Complete a multipart upload
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
    })

    const multiPartUpload = await this.storage.db
      .asSuperUser()
      .findMultipartUpload(UploadId, 'id,version')

    const parts = command.MultipartUpload?.Parts || []

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
      isMultipart: false,
      objectMetadata: metadata,
    })

    await this.storage.db.asSuperUser().deleteMultipartUpload(UploadId)

    return {
      responseBody: {
        CompleteMultipartUpload: {
          Location: resp.location,
          Bucket: resp.bucket,
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
   * Upload a part of a multipart upload
   * @param command
   */
  async uploadPart(command: UploadPartCommandInput) {
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

    const maxFileSize = await getMaxFileSizeLimit(this.storage.db.tenantId, bucket?.file_size_limit)
    const multipart = await this.storage.db.asSuperUser().withTransaction(async (db) => {
      const multipart = await db.findMultipartUpload(
        UploadId,
        'in_progress_size,version,upload_signature',
        {
          forUpdate: true,
        }
      )

      const { progress } = this.decryptUploadSignature(multipart.upload_signature)

      if (progress !== BigInt(multipart.in_progress_size)) {
        throw ERRORS.InvalidUploadSignature()
      }

      const currentProgress = BigInt(multipart.in_progress_size) + BigInt(ContentLength)

      if (currentProgress > maxFileSize) {
        throw ERRORS.EntityTooLarge()
      }

      const signature = this.uploadSignature({ in_progress_size: currentProgress })
      await db.updateMultipartUploadProgress(UploadId, currentProgress, signature)
      return multipart
    })

    const proxy = new PassThrough()

    if (Body instanceof Readable) {
      proxy.on('error', () => {
        Body.unpipe(proxy)
      })

      Body.on('error', () => {
        if (!proxy.closed) {
          proxy.destroy()
        }
      })
    }

    const body = Body instanceof Readable ? Body.pipe(proxy) : Readable.from(Body as Buffer)

    try {
      const uploadPart = await stream.pipeline(
        body,
        new ByteLimitTransformStream(BigInt(ContentLength)),
        async (stream) => {
          return this.storage.backend.uploadPart(
            storageS3Bucket,
            `${this.tenantId}/${Bucket}/${Key}`,
            multipart.version,
            UploadId,
            PartNumber || 0,
            stream as Readable,
            ContentLength
          )
        }
      )

      await this.storage.db.insertUploadPart({
        upload_id: UploadId,
        version: multipart.version,
        part_number: PartNumber || 0,
        etag: uploadPart.ETag || '',
        key: Key as string,
        bucket_id: Bucket,
      })

      return {
        headers: {
          etag: uploadPart.ETag || '',
        },
      }
    } catch (e) {
      await this.storage.db.asSuperUser().withTransaction(async (db) => {
        const multipart = await db.findMultipartUpload(UploadId, 'in_progress_size', {
          forUpdate: true,
        })

        const diff = BigInt(multipart.in_progress_size) - BigInt(ContentLength)
        const signature = this.uploadSignature({ in_progress_size: diff })
        await db.updateMultipartUploadProgress(UploadId, diff, signature)
      })

      throw e
    }
  }

  /**
   * Put an object in a bucket
   * @param command
   */
  async putObject(command: PutObjectCommandInput) {
    const uploader = new Uploader(this.storage.backend, this.storage.db)

    mustBeValidBucketName(command.Bucket)
    mustBeValidKey(command.Key)

    const upload = await uploader.upload(command.Body as any, {
      bucketId: command.Bucket as string,
      objectName: command.Key as string,
      isUpsert: true,
      isMultipart: false,
    })

    return {
      headers: {
        etag: upload.metadata.eTag,
      },
    }
  }

  /**
   * Abort a multipart upload
   * @param command
   */
  async abortMultipartUpload(command: AbortMultipartUploadCommandInput) {
    const { Bucket, Key, UploadId } = command

    if (!UploadId) {
      throw ERRORS.InvalidUploadId()
    }

    await this.storage.backend.abortMultipartUpload(
      storageS3Bucket,
      `${this.tenantId}/${Bucket}/${Key}`,
      UploadId
    )

    await this.storage.db.asSuperUser().deleteMultipartUpload(UploadId)

    return {}
  }

  /**
   * Head Object
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

    const object = await this.storage.from(Bucket).findObject(Key, '*')

    if (!object) {
      throw ERRORS.NoSuchKey(Key)
    }

    return {
      headers: {
        'created-at': (object.created_at as string) || '',
        'cache-control': (object.metadata?.cacheControl as string) || '',
        expires: (object.metadata?.expires as string) || '',
        'content-length': (object.metadata?.size as string) || '',
        'content-type': (object.metadata?.contentType as string) || '',
        etag: (object.metadata?.eTag as string) || '',
        'last-modified': object.updated_at ? new Date(object.updated_at).toISOString() || '' : '',
      },
    }
  }

  /**
   * Get Object
   * @param command
   */
  async getObject(command: GetObjectCommandInput) {
    const bucket = command.Bucket as string
    const key = command.Key as string

    const object = await this.storage.from(bucket).findObject(key, '*')
    const response = await this.storage.backend.getObject(
      storageS3Bucket,
      `${this.tenantId}/${bucket}/${key}`,
      object.version,
      {
        ifModifiedSince: command.IfModifiedSince?.toISOString(),
        ifNoneMatch: command.IfNoneMatch,
        range: command.Range,
      }
    )
    return {
      headers: {
        'cache-control': response.metadata.cacheControl,
        'content-length': response.metadata.contentLength.toString(),
        'content-type': response.metadata.mimetype,
        etag: response.metadata.eTag,
        'last-modified': response.metadata.lastModified?.toUTCString() || '',
      },
      responseBody: response.body,
      statusCode: command.Range ? 206 : 200,
    }
  }

  /**
   * Delete Object
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
   * Delete Multiple Objects
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

    if (!Delete.Objects) {
      throw ERRORS.MissingParameter('Objects')
    }

    if (Delete.Objects.length === 0) {
      return {}
    }

    const deletedResult = await this.storage
      .from(Bucket)
      .deleteObjects(Delete.Objects.map((o) => o.Key || ''))

    return {
      responseBody: {
        DeletedResult: {
          Deleted: Delete.Objects.map((o) => {
            const isDeleted = deletedResult.find((d) => d.name === o.Key)
            if (isDeleted) {
              return {
                Deleted: {
                  Key: o.Key,
                },
              }
            }

            return {
              Error: {
                Key: o.Key,
                Code: 'AccessDenied',
                Message:
                  "You do not have permission to delete this object or the object doesn't exists",
              },
            }
          }),
        },
      },
    }
  }

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

    const sourceBucket = CopySource.split('/').shift()
    const sourceKey = CopySource.split('/').slice(1).join('/')

    if (!sourceBucket) {
      throw ERRORS.MissingParameter('CopySource')
    }

    if (!sourceKey) {
      throw ERRORS.MissingParameter('CopySource')
    }

    const object = await this.storage.from(sourceBucket).findObject(sourceKey, '*')

    if (!object) {
      throw ERRORS.NoSuchKey(sourceKey)
    }

    const copyResult = await this.storage
      .from(sourceBucket)
      .copyObject(sourceKey, Bucket, Key, object.owner, {
        ifMatch: command.CopySourceIfMatch,
        ifNoneMatch: command.CopySourceIfNoneMatch,
        ifModifiedSince: command.CopySourceIfModifiedSince,
        ifUnmodifiedSince: command.CopySourceIfUnmodifiedSince,
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

  async listParts(command: ListPartsCommandInput) {
    if (!command.UploadId) {
      throw ERRORS.MissingParameter('UploadId')
    }

    await this.storage.db.asSuperUser().findMultipartUpload(command.UploadId, 'id')

    const maxParts = command.MaxParts || 1000

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
          MaxParts: command.MaxParts || 1000,
          IsTruncated: isTruncated,
          Part: parts,
        },
      },
    }
  }

  protected uploadSignature({ in_progress_size }: { in_progress_size: BigInt }) {
    return `${encrypt('progress:' + in_progress_size.toString())}`
  }

  protected decryptUploadSignature(signature: string) {
    const originalSignature = decrypt(signature)
    const [_, value] = originalSignature.split(':')

    return {
      progress: BigInt(value),
    }
  }
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
