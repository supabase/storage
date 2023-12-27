import { CancellationContext, ERRORS, EVENTS } from '@tus/server'
import { DeleteHandler as BaseDeleteHandler } from '@tus/server/dist/handlers/DeleteHandler'
import http from 'node:http'

export class DeleteHandler extends BaseDeleteHandler {
  async send(req: http.IncomingMessage, res: http.ServerResponse, context: CancellationContext) {
    const id = this.getFileIdFromRequest(req)
    if (!id) {
      throw ERRORS.FILE_NOT_FOUND
    }

    if (this.options.onIncomingRequest) {
      await this.options.onIncomingRequest(req, res, id)
    }

    const lock = await this.acquireLock(req, id, context)
    try {
      const upload = await this.store.getUpload(id)
      if (upload.offset === upload.size) {
        throw {
          status_code: 400,
          body: 'Cannot terminate an already completed upload',
        }
      }
      await this.store.remove(id)
    } finally {
      await lock.unlock()
    }
    const writtenRes = this.write(res, 204, {})
    this.emit(EVENTS.POST_TERMINATE, req, writtenRes, id)
    return writtenRes
  }
}
