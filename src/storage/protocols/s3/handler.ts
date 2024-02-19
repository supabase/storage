import { Storage } from '../../storage'
import { getConfig } from '../../../config'
import { Uploader } from '../../uploader'
import {
  CompleteMultipartUploadCommandInput,
  CreateMultipartUploadCommandInput,
  GetObjectCommandInput,
  HeadObjectCommandInput,
  ListObjectsV2CommandInput,
  PutObjectCommandInput,
  UploadPartCommandInput,
} from '@aws-sdk/client-s3'
import { Readable } from 'stream'
import { isValidBucketName, isValidKey, mustBeValidBucketName, mustBeValidKey } from '../../limits'

const { storageS3Region, storageS3Bucket } = getConfig()

export class S3ProtocolHandler {
  constructor(protected readonly storage: Storage, protected readonly tenantId: string) {}

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

  async getBucketLocation() {
    return {
      responseBody: {
        LocationConstraint: {
          LocationConstraint: storageS3Region,
        },
      },
    }
  }

  async listBuckets() {
    const buckets = await this.storage.listBuckets('name,created_at')

    return {
      responseBody: {
        ListAllMyBucketsResult: {
          Buckets: toXmlList(
            'Bucket',
            buckets.map((bucket) => ({
              Name: bucket.name,
              CreationDate: bucket.created_at
                ? new Date(bucket.created_at || '').toISOString().replace('Z', '+00:00')
                : undefined,
            }))
          ),
        },
      },
    }
  }

  async createBucket(Bucket: string, isPublic: boolean) {
    if (!Bucket) {
      throw new Error('Bucket name is required')
    }

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

  async deleteBucket(name: string) {
    await this.storage.deleteBucket(name)

    return {
      statusCode: 204,
    }
  }

  async listObjectsV2(command: ListObjectsV2CommandInput) {
    const continuationToken = command.ContinuationToken
    const startAfter = command.StartAfter
    const encodingType = command.EncodingType
    const delimiter = command.Delimiter
    const prefix = command.Prefix
    const maxKeys = command.MaxKeys
    const bucket = command.Bucket!

    const limit = maxKeys || 200
    const offset = continuationToken ? parseInt(continuationToken.split('=')[1]) : 0

    const objects = await this.storage.from(bucket).searchObjects(prefix || '', {
      limit: limit,
      offset: offset,
      sortBy: { column: 'created_at', order: 'desc' },
    })

    const commonPrefeixes = objects
      .map((object) => {
        if (object.id === null) {
          return { Prefix: object.name + '/' }
        }
      })
      .filter((e) => e)

    const contents =
      objects
        .filter((o) => o.id)
        .map((o) => ({
          Key: o.name,
          LastModified: o.updated_at
            ? new Date(o.updated_at).toISOString().replace('Z', '+00:00')
            : undefined,
          ETag: o.metadata?.eTag,
          Size: o.metadata?.size,
          StorageClass: 'STANDARD',
        })) || []

    const isTruncated = objects.length === 0 || objects.length < limit
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
        KeyCount: objects.length,
        CommonPrefixes: commonPrefeixes,
        NextContinuationToken: !isTruncated ? `offset=${offset + limit}` : undefined,
        StartAfter: offset,
      },
    }

    return {
      responseBody: response,
    }
  }

  async createMultiPartUpload(command: CreateMultipartUploadCommandInput) {
    const uploader = new Uploader(this.storage.backend, this.storage.db)

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

  async completeMultiPartUpload(command: CompleteMultipartUploadCommandInput) {
    const uploader = new Uploader(this.storage.backend, this.storage.db)
    const { Bucket, Key, UploadId } = command

    await uploader.canUpload({
      bucketId: Bucket as string,
      objectName: Key as string,
      isUpsert: true,
    })

    const resp = await this.storage.backend.completeMultipartUpload(
      storageS3Bucket,
      `${this.tenantId}/${Bucket}/${Key}`,
      UploadId as string,
      command.MultipartUpload?.Parts || []
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

    return {
      responseBody: {
        CompleteMultipartUpload: {
          Location: resp.location,
          Bucket: resp.bucket,
          ChecksumCRC32: resp.ChecksumCRC32,
          ChecksumCRC32C: resp.ChecksumCRC32,
          ChecksumSHA1: resp.ChecksumSHA1,
          ChecksumSHA256: resp.ChecksumSHA256,
          ETag: resp.ETag,
        },
      },
    }
  }

  async uploadPart(command: UploadPartCommandInput) {
    const { Bucket, PartNumber, UploadId, Key, Body } = command

    if (!UploadId) {
      throw new Error('UploadId is required')
    }

    const uploadPart = await this.storage.backend.uploadPart(
      storageS3Bucket,
      `${this.tenantId}/${Bucket}/${Key}`,
      UploadId,
      PartNumber || 0,
      Body as string | Uint8Array | Buffer | Readable,
      command.ContentLength
    )

    return {
      headers: {
        etag: uploadPart.ETag || '',
      },
    }
  }

  async putObject(command: PutObjectCommandInput) {
    const uploader = new Uploader(this.storage.backend, this.storage.db)
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

  async headObject(command: HeadObjectCommandInput) {
    const { Bucket, Key } = command

    if (!Bucket || !Key) {
      throw new Error('Bucket and Key are required')
    }

    const object = await this.storage.from(Bucket).findObject(Key, '*')

    if (!object) {
      throw new Error('Object not found')
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

  async getObject(command: GetObjectCommandInput) {
    const bucket = command.Bucket as string
    const key = command.Key as string

    mustBeValidBucketName(bucket || '', 'Invalid Bucket Name')
    mustBeValidKey(key || '', 'Invalid Key')

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
        'last-modified': response.metadata.lastModified?.toISOString() || '',
      },
      responseBody: response.body,
    }
  }
}

function toXmlList<T extends Array<any>>(name: string, list: T) {
  if (list.length === 0) {
    return undefined
  }

  if (list.length === 1) {
    return list.map((e) => ({ [name]: e }))
  }

  return list
}
