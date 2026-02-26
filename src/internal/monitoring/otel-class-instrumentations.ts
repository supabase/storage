import { S3Client } from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { TenantConnection } from '@internal/database'
import { Event as QueueBaseEvent } from '@internal/queue'
import { Permit, Semaphore } from '@shopify/semaphore'
import { S3Backend } from '@storage/backend'
import { StorageKnexDB } from '@storage/database'
import { ObjectStorage } from '@storage/object'
import { PgLock } from '@storage/protocols/tus'
import { Storage } from '@storage/storage'
import { Uploader } from '@storage/uploader'
import { S3Store } from '@tus/s3-store'
import { StreamSplitter } from '@tus/server'
import { ClassInstrumentation } from './otel-instrumentation'

export const classInstrumentations = [
  new ClassInstrumentation({
    targetClass: Storage,
    enabled: true,
    methodsToInstrument: [
      'findBucket',
      'listBuckets',
      'createBucket',
      'updateBucket',
      'countObjects',
      'deleteBucket',
      'emptyBucket',
      'healthcheck',
    ],
  }),
  new ClassInstrumentation({
    targetClass: ObjectStorage,
    enabled: true,
    methodsToInstrument: [
      'uploadNewObject',
      'uploadOverridingObject',
      'deleteObject',
      'deleteObjects',
      'updateObjectMetadata',
      'updateObjectOwner',
      'findObject',
      'findObjects',
      'copyObject',
      'moveObject',
      'searchObjects',
      'listObjectsV2',
      'signObjectUrl',
      'signObjectUrls',
      'signUploadObjectUrl',
      'verifyObjectSignature',
    ],
  }),
  new ClassInstrumentation({
    targetClass: Uploader,
    enabled: true,
    methodsToInstrument: ['canUpload', 'prepareUpload', 'upload', 'completeUpload'],
  }),
  new ClassInstrumentation({
    targetClass: QueueBaseEvent,
    enabled: true,
    methodsToInstrument: ['send', 'batchSend'],
    setName: (name, attrs, eventClass) => {
      if (attrs.constructor.name) {
        return name + '.' + eventClass.constructor.name
      }
      return name
    },
  }),
  new ClassInstrumentation({
    targetClass: S3Backend,
    enabled: true,
    methodsToInstrument: [
      'getObject',
      'putObject',
      'deleteObject',
      'listObjects',
      'copyObject',
      'headObject',
      'createMultipartUpload',
      'uploadPart',
      'completeMultipartUpload',
      'abortMultipartUpload',
      'listMultipartUploads',
      'listParts',
      'getSignedUrl',
      'createBucket',
      'deleteBucket',
      'listBuckets',
      'getBucketLocation',
      'getBucketVersioning',
      'putBucketVersioning',
      'getBucketLifecycleConfiguration',
      'putBucketLifecycleConfiguration',
      'deleteBucketLifecycle',
      'uploadObject',
      'privateAssetUrl',
    ],
  }),
  new ClassInstrumentation({
    targetClass: StorageKnexDB,
    enabled: true,
    methodsToInstrument: ['runQuery'],
    setName: (name, attrs) => {
      if (attrs.queryName) {
        return name + '.' + attrs.queryName
      }
      return name
    },
    setAttributes: {
      runQuery: (queryName) => {
        return {
          queryName,
        }
      },
    },
  }),
  new ClassInstrumentation({
    targetClass: TenantConnection,
    enabled: true,
    methodsToInstrument: ['transaction', 'setScope'],
  }),
  new ClassInstrumentation({
    targetClass: S3Store,
    enabled: true,
    methodsToInstrument: [
      'write',
      'create',
      'remove',
      'getUpload',
      'declareUploadLength',
      'uploadIncompletePart',
      'uploadPart',
      'downloadIncompletePart',
      'uploadParts',
    ],
    setName: (name) => 'Tus.' + name,
  }),
  new ClassInstrumentation({
    targetClass: StreamSplitter,
    enabled: true,
    methodsToInstrument: ['emitEvent'],
    setName: (name: string, attrs: any) => {
      if (attrs.event) {
        return name + '.' + attrs.event
      }
      return name
    },
    setAttributes: {
      emitEvent(event) {
        return {
          part: this.part as any,
          event,
        }
      },
    },
  }),
  new ClassInstrumentation({
    targetClass: PgLock,
    enabled: true,
    methodsToInstrument: ['lock', 'unlock', 'acquireLock'],
  }),
  new ClassInstrumentation({
    targetClass: Semaphore,
    enabled: true,
    methodsToInstrument: ['acquire'],
  }),
  new ClassInstrumentation({
    targetClass: Permit,
    enabled: true,
    methodsToInstrument: ['release'],
  }),
  new ClassInstrumentation({
    targetClass: S3Client,
    enabled: true,
    methodsToInstrument: ['send'],
    setAttributes: {
      send: (command) => {
        return {
          operation: command.constructor.name as string,
        }
      },
    },
    setName: (name, attrs) => 'S3.' + attrs.operation,
  }),
  new ClassInstrumentation({
    targetClass: Upload,
    enabled: true,
    methodsToInstrument: [
      'done',
      '__uploadUsingPut',
      '__createMultipartUpload',
      'markUploadAsAborted',
    ],
  }),
]
