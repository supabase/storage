export const ROUTE_OPERATIONS = {
  // Bucket
  CREATE_BUCKET: 'storage.bucket.create',
  DELETE_BUCKET: 'storage.bucket.delete',
  EMPTY_BUCKET: 'storage.bucket.empty',
  LIST_BUCKET: 'storage.bucket.list',
  GET_BUCKET: 'storage.bucket.get',
  UPDATE_BUCKET: 'storage.bucket.update',

  // Object
  COPY_OBJECT: 'storage.object.copy',
  CREATE_OBJECT: 'storage.object.upload',
  DELETE_OBJECT: 'storage.object.delete',
  DELETE_OBJECTS: 'storage.object.delete_many',
  GET_PUBLIC_OBJECT: 'storage.object.get_public',
  GET_AUTH_OBJECT: 'storage.object.get_authenticated',
  INFO_AUTH_OBJECT: 'storage.object.info_authenticated',
  INFO_PUBLIC_OBJECT: 'storage.object.info_public',
  GET_SIGNED_OBJECT: 'storage.object.get_signed',
  SIGN_UPLOAD_URL: 'storage.object.sign_upload_url',
  SIGN_OBJECT_URL: 'storage.object.sign',
  SIGN_OBJECT_URLS: 'storage.object.sign_many',
  LIST_OBJECTS: 'storage.object.list',
  MOVE_OBJECT: 'storage.object.move',
  UPDATE_OBJECT: 'storage.object.upload_update',
  UPLOAD_SIGN_OBJECT: 'storage.object.upload_signed',

  // Image Transformation
  RENDER_AUTH_IMAGE: 'storage.render.image_authenticated',
  RENDER_PUBLIC_IMAGE: 'storage.render.image_public',
  RENDER_SIGNED_IMAGE: 'storage.render.image_sign',

  // S3
  S3_ABORT_MULTIPART: 'storage.s3.upload.abort_multipart',
  S3_COMPLETE_MULTIPART: 'storage.s3.upload.complete_multipart',
  S3_CREATE_MULTIPART: 'storage.s3.upload.create_multipart',
  S3_LIST_MULTIPART: 'storage.s3.upload.list_multipart',
  S3_LIST_PARTS: 'storage.s3.upload.list_parts',
  S3_UPLOAD_PART: 'storage.s3.upload.part',
  S3_UPLOAD_PART_COPY: 'storage.s3.upload.part_copy',
  S3_UPLOAD: 'storage.s3.upload',

  // S3 Object
  S3_COPY_OBJECT: 'storage.s3.object.copy',
  S3_GET_OBJECT: 'storage.s3.object.get',
  S3_LIST_OBJECT: 'storage.s3.object.list',
  S3_HEAD_OBJECT: 'storage.s3.object.info',
  S3_DELETE_OBJECTS: 'storage.s3.object.delete_many',
  S3_DELETE_OBJECT: 'storage.s3.object.delete',
  S3_GET_OBJECT_TAGGING: 'storage.s3.object.get_tagging',

  // S3 Bucket
  S3_CREATE_BUCKET: 'storage.s3.bucket.create',
  S3_DELETE_BUCKET: 'storage.s3.bucket.delete',
  S3_GET_BUCKET: 'storage.s3.bucket.get',
  S3_HEAD_BUCKET: 'storage.s3.bucket.head',
  S3_LIST_BUCKET: 'storage.s3.bucket.list',
  S3_GET_BUCKET_LOCATION: 'storage.s3.bucket.get_location',
  S3_GET_BUCKET_VERSIONING: 'storage.s3.bucket.get_versioning',

  // Tus
  TUS_CREATE_UPLOAD: 'storage.tus.upload.create',
  TUS_UPLOAD_PART: 'storage.tus.upload.part',
  TUS_GET_UPLOAD: 'storage.tus.upload.get',
  TUS_DELETE_UPLOAD: 'storage.tus.upload.delete',
  TUS_OPTIONS: 'storage.tus.options',
}
