import http from 'http'
import { isRenderableError, Storage } from '../../../storage'
import { Upload } from '@tus/server'
import { getConfig } from '../../../config'
import { randomUUID } from 'crypto'

const { globalS3Bucket } = getConfig()

export type MultiPartRequest = http.IncomingMessage & {
  upload: {
    storage: Storage
    owner?: string
    tenantId: string
    isNew: boolean
  }
}

export function namingFunction(rawReq: http.IncomingMessage) {
  const req = rawReq as MultiPartRequest

  if (!req.url) {
    throw new Error('no url set')
  }

  const metadataHeader = req.headers['upload-metadata']

  if (typeof metadataHeader !== 'string') {
    throw new Error('no metadata')
  }

  // TODO: validate params
  const params = metadataHeader.split(',').reduce((all, param) => {
    const [k, v] = param.split(' ')
    const key = k
    const value = Buffer.from(v, 'base64').toString('utf8')
    all[key] = value
    return all
  }, {} as any)

  // const [bucket, ...objNameParts] = req.url.split('/')

  const version = randomUUID()

  return `${req.upload.tenantId}/${params.bucketName}/${params.objectName}/${version}`
}

export async function onCreate(
  rawReq: http.IncomingMessage,
  res: http.ServerResponse,
  upload: Upload
): Promise<http.ServerResponse> {
  try {
    const [, bucket, ...objParts] = upload.id.split('/')
    const version = objParts.pop()
    const objectName = objParts.join('/')

    const req = rawReq as MultiPartRequest
    const isUpsert = req.headers['x-upsert'] === 'true'
    const storage = req.upload.storage

    await storage.from(bucket).findOrCreateObjectForUpload({
      version,
      owner: req.upload.owner,
      objectName: objectName,
      isUpsert,
    })

    return res
  } catch (e) {
    if (isRenderableError(e)) {
      ;(e as any).status_code = parseInt(e.render().statusCode, 10)
    }
    throw e
  }
}

export async function onUploadFinish(
  rawReq: http.IncomingMessage,
  res: http.ServerResponse,
  upload: Upload
) {
  const [, bucket, ...objParts] = upload.id.split('/')
  const version = objParts.pop()
  const objectName = objParts.join('/')

  // console.log('on finish', upload)

  const req = rawReq as MultiPartRequest

  try {
    const s3Key = `${req.upload.tenantId}/${bucket}/${objectName}`
    const metadata = await req.upload.storage.backend.headObject(globalS3Bucket, s3Key, version)

    await req.upload.storage.from(bucket).completeObjectUpload({
      objectName: objectName,
      version: version,
      objectMetadata: metadata,
      isUpsert: req.headers['x-upsert'] === 'true',
    })
  } catch (e) {
    if (isRenderableError(e)) {
      ;(e as any).status_code = parseInt(e.render().statusCode, 10)
      ;(e as any).body = e.render().message
    }
    throw e
  }

  return res
}
