import { PatchHandler } from '@tus/server/handlers/PatchHandler'
import http from 'http'
import { ALLOWED_HEADERS, ALLOWED_METHODS, ERRORS, MAX_AGE } from '@tus/server'
import { HeadHandler } from '@tus/server/handlers/HeadHandler'
import { PostHandler } from '@tus/server/handlers/PostHandler'
import { isRenderableError, Storage } from '../../../storage'
import { MultiPartRequest } from './lifecycle'
import { Database } from '../../../storage/database'
import { OptionsHandler } from '@tus/server/handlers/OptionsHandler'
import { UploadId } from './upload-id'
import { getFileSizeLimit } from '../../../storage/limits'
import { Uploader } from '../../../storage/uploader'

const reExtractFileID = /([^/]+)\/?$/

export class Patch extends PatchHandler {
  getFileIdFromRequest(rawRwq: http.IncomingMessage) {
    return getFileIdFromRequest(rawRwq, this.options.path)
  }

  async send(rawReq: http.IncomingMessage, res: http.ServerResponse) {
    const req = rawReq as MultiPartRequest

    const id = this.getFileIdFromRequest(req)
    if (id === false) {
      throw ERRORS.FILE_NOT_FOUND
    }

    const uploadID = UploadId.fromString(id)

    const uploader = new Uploader(req.upload.storage.backend, req.upload.storage.db)

    try {
      await uploader.canUpload({
        bucketId: uploadID.bucket,
        objectName: uploadID.objectName,
        owner: req.upload.owner,
        isUpsert: req.headers['x-upsert'] === 'true',
      })

      return await lock(uploadID, req.upload.storage, (db) => {
        req.upload.storage = new Storage(req.upload.storage.backend, db)
        return super.send(req, res)
      })
    } catch (e) {
      ;(res as any).executionError = e
      ;(res as any).internalHandler = 'PatchHandler'

      if (isRenderableError(e)) {
        const statusCode = parseInt(e.render().statusCode, 10)
        ;(e as any).status_code = statusCode
        ;(e as any).body = statusCode !== 500 ? e.render().message : 'Internal Server Error'
      }

      throw e
    } finally {
      await req.upload.db.dispose().catch(() => {
        req.log.error('failed disposing connection')
      })
    }
  }
}

export class Head extends HeadHandler {
  getFileIdFromRequest(rawRwq: http.IncomingMessage) {
    return getFileIdFromRequest(rawRwq, this.options.path)
  }

  async send(rawReq: http.IncomingMessage, res: http.ServerResponse) {
    const req = rawReq as MultiPartRequest

    const id = this.getFileIdFromRequest(req)
    if (id === false) {
      throw ERRORS.FILE_NOT_FOUND
    }

    const uploadID = UploadId.fromString(id)

    const uploader = new Uploader(req.upload.storage.backend, req.upload.storage.db)

    try {
      await uploader.canUpload({
        bucketId: uploadID.bucket,
        objectName: uploadID.objectName,
        owner: req.upload.owner,
        isUpsert: req.headers['x-upsert'] === 'true',
      })

      return await lock(
        uploadID,
        req.upload.storage,
        (db) => {
          req.upload.storage = new Storage(req.upload.storage.backend, db)
          return super.send(req, res)
        },
        true
      )
    } catch (e) {
      ;(res as any).executionError = e
      ;(res as any).internalHandler = 'HeadHandler'

      if (isRenderableError(e)) {
        const statusCode = parseInt(e.render().statusCode, 10)
        ;(e as any).status_code = statusCode
        ;(e as any).body = statusCode !== 500 ? e.render().message : 'Internal Server Error'
      }
      throw e
    } finally {
      await req.upload.db.dispose().catch(() => {
        req.log.error('failed disposing connection')
      })
    }
  }
}

export class Post extends PostHandler {
  getFileIdFromRequest(rawRwq: http.IncomingMessage) {
    return getFileIdFromRequest(rawRwq, this.options.path)
  }

  async send(rawReq: http.IncomingMessage, res: http.ServerResponse) {
    const req = rawReq as MultiPartRequest

    if ('upload-concat' in req.headers && !this.store.hasExtension('concatentation')) {
      throw ERRORS.UNSUPPORTED_CONCATENATION_EXTENSION
    }

    const upload_defer_length = req.headers['upload-defer-length'] as string | undefined

    if (
      upload_defer_length !== undefined && // Throw error if extension is not supported
      !this.store.hasExtension('creation-defer-length')
    ) {
      throw ERRORS.UNSUPPORTED_CREATION_DEFER_LENGTH_EXTENSION
    }

    try {
      const id = this.options.namingFunction(req)
      const uploadID = UploadId.fromString(id)

      return await lock(uploadID, req.upload.storage, (db) => {
        req.upload.storage = new Storage(req.upload.storage.backend, db)
        return super.send(req, res)
      })
    } catch (e) {
      ;(res as any).executionError = e
      ;(res as any).internalHandler = 'PostHandler'

      if (isRenderableError(e)) {
        const statusCode = parseInt(e.render().statusCode, 10)
        ;(e as any).status_code = statusCode
        ;(e as any).body = statusCode !== 500 ? e.render().message : 'Internal Server Error'
      }

      throw e
    } finally {
      await req.upload.db.dispose().catch(() => {
        req.log.error('failed disposing connection')
      })
    }
  }

  generateUrl(rawReq: http.IncomingMessage, id: string) {
    const req = rawReq as MultiPartRequest
    id = id.split('/').slice(1).join('/')

    // Enforce https in production
    if (process.env.NODE_ENV === 'production') {
      req.headers['x-forwarded-proto'] = 'https'
    }

    id = Buffer.from(id, 'utf-8').toString('base64url')
    return super.generateUrl(req, id)
  }
}

export class Options extends OptionsHandler {
  getFileIdFromRequest(rawRwq: http.IncomingMessage) {
    return getFileIdFromRequest(rawRwq, this.options.path)
  }

  async send(rawReq: http.IncomingMessage, res: http.ServerResponse) {
    const req = rawReq as MultiPartRequest

    try {
      res.setHeader('Access-Control-Allow-Origin', '*')
      res.setHeader('Access-Control-Allow-Methods', ALLOWED_METHODS)
      res.setHeader(
        'Access-Control-Allow-Headers',
        ALLOWED_HEADERS + ', X-Upsert, Upload-Expires, ApiKey'
      )
      res.setHeader('Access-Control-Max-Age', MAX_AGE)
      if (this.store.extensions.length > 0) {
        res.setHeader('Tus-Extension', this.store.extensions.join(','))
      }

      let uploadID: UploadId | undefined

      const urlParts = rawReq.url?.split('/') || []
      const pathParts = this.options.path.split('/')

      if (urlParts.length > pathParts.length) {
        const id = this.getFileIdFromRequest(rawReq)

        if (!id) {
          throw ERRORS.FILE_NOT_FOUND
        }

        uploadID = UploadId.fromString(id)
      }

      const fileSizeLimit = await getFileSizeLimit(req.upload.tenantId)

      if (!uploadID) {
        res.setHeader('Tus-Max-Size', fileSizeLimit.toString())
        return this.write(res, 204)
      }

      const bucket = await req.upload.storage
        .asSuperUser()
        .findBucket(uploadID.bucket, 'id, file_size_limit')

      if (bucket.file_size_limit && bucket.file_size_limit > fileSizeLimit) {
        res.setHeader('Tus-Max-Size', fileSizeLimit.toString())
        return this.write(res, 204)
      }

      if (bucket.file_size_limit) {
        res.setHeader('Tus-Max-Size', bucket.file_size_limit.toString())
        return this.write(res, 204)
      }

      res.setHeader('Tus-Max-Size', fileSizeLimit.toString())
      return this.write(res, 204)
    } catch (e) {
      ;(res as any).executionError = e
      ;(res as any).internalHandler = 'OptionHandler'

      if (isRenderableError(e)) {
        const statusCode = parseInt(e.render().statusCode, 10)
        ;(e as any).status_code = statusCode
        ;(e as any).body = statusCode !== 500 ? e.render().message : 'Internal Server Error'
      }

      throw e
    } finally {
      await req.upload.db.dispose().catch(() => {
        req.log.error('failed disposing connection')
      })
    }
  }
}

function getFileIdFromRequest(rawRwq: http.IncomingMessage, path: string) {
  const req = rawRwq as MultiPartRequest
  const match = reExtractFileID.exec(req.url as string)

  if (!match || path.includes(match[1])) {
    return false
  }

  const idMatch = Buffer.from(match[1], 'base64url').toString('utf-8')
  return req.upload.tenantId + '/' + idMatch
}

async function lock<T extends (db: Database) => any>(
  id: UploadId,
  storage: Storage,
  fn: T,
  wait?: boolean
) {
  try {
    return await storage.db.withTransaction(async (db) => {
      if (!wait) {
        await db.mustLockObject(id.bucket, id.objectName, id.version)
      } else {
        await db.waitObjectLock(id.bucket, id.objectName, id.version)
      }
      return await fn(db)
    })
  } catch (e) {
    if (isRenderableError(e)) {
      const statusCode = parseInt(e.render().statusCode, 10)
      ;(e as any).status_code = statusCode
      ;(e as any).body = statusCode !== 500 ? e.render().message : 'Internal Server Error'
    }

    throw e
  }
}
