import { PatchHandler } from '@tus/server/dist/handlers/PatchHandler'
import http from 'http'
import { ERRORS } from '@tus/server'
import { HeadHandler } from '@tus/server/dist/handlers/HeadHandler'
import { PostHandler } from '@tus/server/dist/handlers/PostHandler'
import { isRenderableError, Storage } from '../../../storage'
import { MultiPartRequest } from './lifecycle'
import { Database } from '../../../storage/database'

const reExtractFileID = /([^/]+)\/?$/

async function lock<T extends (db: Database) => any>(id: string, storage: Storage, fn: T) {
  const [, bucket, ...objParts] = id.split('/')
  const version = objParts.pop()
  const objectName = objParts.join('/')

  try {
    return await storage.db.withTransaction(async (db) => {
      await db.mustLockObject(bucket, objectName, version)
      return await fn(db)
    })
  } catch (e) {
    if (isRenderableError(e)) {
      ;(e as any).status_code = e.render().statusCode
    }

    throw e
  }
}

export class Patch extends PatchHandler {
  getFileIdFromRequest(rawRwq: http.IncomingMessage) {
    const req = rawRwq as MultiPartRequest
    const match = reExtractFileID.exec(req.url as string)

    if (!match || this.options.path.includes(match[1])) {
      return false
    }

    return decodeURIComponent(req.upload.tenantId + '/' + match[1])
  }

  async send(rawReq: http.IncomingMessage, res: http.ServerResponse) {
    const req = rawReq as MultiPartRequest

    const id = this.getFileIdFromRequest(req)
    if (id === false) {
      throw ERRORS.FILE_NOT_FOUND
    }

    return lock(id, req.upload.storage, (db) => {
      req.upload.storage = new Storage(req.upload.storage.backend, db)
      return super.send(req, res)
    })
  }
}

export class Head extends HeadHandler {
  getFileIdFromRequest(rawRwq: http.IncomingMessage) {
    const req = rawRwq as MultiPartRequest
    const match = reExtractFileID.exec(req.url as string)

    if (!match || this.options.path.includes(match[1])) {
      return false
    }

    return decodeURIComponent(req.upload.tenantId + '/' + match[1])
  }

  async send(rawReq: http.IncomingMessage, res: http.ServerResponse) {
    const req = rawReq as MultiPartRequest

    const id = this.getFileIdFromRequest(req)
    if (id === false) {
      throw ERRORS.FILE_NOT_FOUND
    }

    const result = await lock(id, req.upload.storage, (db) => {
      req.upload.storage = new Storage(req.upload.storage.backend, db)
      return super.send(req, res)
    })
    console.timeEnd('patch')
    return result
  }
}

export class Post extends PostHandler {
  getFileIdFromRequest(rawRwq: http.IncomingMessage) {
    const req = rawRwq as MultiPartRequest
    const match = reExtractFileID.exec(req.url as string)

    if (!match || this.options.path.includes(match[1])) {
      return false
    }

    return decodeURIComponent(req.upload.tenantId + '/' + match[1])
  }

  async send(rawReq: http.IncomingMessage, res: http.ServerResponse) {
    const req = rawReq as MultiPartRequest

    if ('upload-concat' in req.headers && !this.store.hasExtension('concatentation')) {
      throw ERRORS.UNSUPPORTED_CONCATENATION_EXTENSION
    }

    const upload_length = req.headers['upload-length'] as string | undefined
    const upload_defer_length = req.headers['upload-defer-length'] as string | undefined

    if (
      upload_defer_length !== undefined && // Throw error if extension is not supported
      !this.store.hasExtension('creation-defer-length')
    ) {
      throw ERRORS.UNSUPPORTED_CREATION_DEFER_LENGTH_EXTENSION
    }

    if ((upload_length === undefined) === (upload_defer_length === undefined)) {
      throw ERRORS.INVALID_LENGTH
    }

    try {
      const id = this.options.namingFunction(req)
      console.time('post')
      const result = await lock(id, req.upload.storage, (db) => {
        req.upload.storage = new Storage(req.upload.storage.backend, db)
        return super.send(req, res)
      })
      console.timeEnd('post')

      return result
    } catch (error) {
      throw ERRORS.FILE_WRITE_ERROR
    }
  }

  generateUrl(rawReq: http.IncomingMessage, id: string) {
    const req = rawReq as MultiPartRequest
    id = id.split('/').slice(1).join('/')

    return super.generateUrl(req, id)
  }
}
