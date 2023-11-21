import http from 'node:http'
import { OptionsHandler } from '@tus/server/handlers/OptionsHandler'
import { ALLOWED_HEADERS, ALLOWED_METHODS, MAX_AGE } from '@tus/server'

export class TusOptionsHandler extends OptionsHandler {
  async send(req: http.IncomingMessage, res: http.ServerResponse) {
    const maxSize = await this.getConfiguredMaxSize(req, '')

    if (maxSize > 0) {
      res.setHeader('Tus-Max-Size', maxSize)
    }

    res.setHeader('Access-Control-Allow-Methods', ALLOWED_METHODS)
    res.setHeader(
      'Access-Control-Allow-Headers',
      ALLOWED_HEADERS + ',X-Trace-Id,X-Upsert, Upload-Expires, ApiKey'
    )
    res.setHeader('Access-Control-Max-Age', MAX_AGE)
    if (this.store.extensions.length > 0) {
      res.setHeader('Tus-Extension', this.store.extensions.join(','))
    }

    return this.write(res, 204)
  }
}
