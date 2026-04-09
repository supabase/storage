import { Event } from '../internal/queue/event'
import { ObjectMetadata } from '../storage/backend'
import { ObjectCreatedPutEvent } from '../storage/events/lifecycle/object-created'
import { ObjectRemoved } from '../storage/events/lifecycle/object-removed'
import { ObjectUpdatedMetadata } from '../storage/events/lifecycle/object-updated'
import type { Obj } from '../storage/schemas'

type TypeCheckedPayload = {
  tenant: {
    ref: string
    host: string
  }
  name: string
  bucketId: string
}

class TypeCheckedEvent extends Event<TypeCheckedPayload> {
  protected static queueName = 'type-checked-event'
}

const tenant = {
  ref: 'tenant-ref',
  host: 'tenant-host',
}

const metadata: ObjectMetadata = {
  cacheControl: 'no-cache',
  contentLength: 1,
  size: 1,
  mimetype: 'text/plain',
  lastModified: new Date('2026-04-07T00:00:00.000Z'),
  eTag: 'etag',
}

const persistedMetadata: NonNullable<Obj['metadata']> = {
  cacheControl: 'no-cache',
  contentLength: 1,
  size: 1,
  mimetype: 'text/plain',
  lastModified: '2026-04-07T00:00:00.000Z',
  eTag: 'etag',
}

void TypeCheckedEvent.send({
  tenant,
  name: 'object-name',
  bucketId: 'bucket-id',
})

function _typecheckRequiredFields() {
  // @ts-expect-error required event payload fields must stay required
  void TypeCheckedEvent.send({
    tenant,
  })
}

void ObjectCreatedPutEvent.sendWebhook({
  tenant,
  name: 'object-name',
  version: 'object-version',
  bucketId: 'bucket-id',
  metadata,
  uploadType: 'standard',
})

function _typecheckUploadTypeRequired() {
  // @ts-expect-error upload-created events must keep uploadType required
  void ObjectCreatedPutEvent.sendWebhook({
    tenant,
    name: 'object-name',
    version: 'object-version',
    bucketId: 'bucket-id',
    metadata,
  })
}

void ObjectRemoved.sendWebhook({
  tenant,
  name: 'object-name',
  version: null,
  bucketId: 'bucket-id',
  metadata: persistedMetadata,
})

function _typecheckRemovedVersionRequired() {
  // @ts-expect-error removed events must keep a version field, even when the value is null
  void ObjectRemoved.sendWebhook({
    tenant,
    name: 'object-name',
    bucketId: 'bucket-id',
    metadata: persistedMetadata,
  })
}

void ObjectUpdatedMetadata.sendWebhook({
  tenant,
  name: 'object-name',
  version: null,
  bucketId: 'bucket-id',
  metadata,
})

function _typecheckUpdatedVersionRequired() {
  // @ts-expect-error metadata-updated events must keep a version field, even for legacy rows
  void ObjectUpdatedMetadata.sendWebhook({
    tenant,
    name: 'object-name',
    bucketId: 'bucket-id',
    metadata,
  })
}
