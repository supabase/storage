import { DefaultUploadIdGenerator } from '@tus/server'
import http from 'http'
import { MultiPartRequest } from './lifecycle'
import { DefaultUploadIdGeneratorOptions } from '@tus/server/models/UploadIdGenerator'

const reExtractFileID = /([^/]+)\/?$/

export class TusURLGenerator extends DefaultUploadIdGenerator {
  constructor(private readonly genOptions: DefaultUploadIdGeneratorOptions) {
    super(genOptions)
  }

  generateUrl(rawReq: http.IncomingMessage, id: string): string {
    const req = rawReq as MultiPartRequest
    id = id.split('/').slice(1).join('/')

    // Enforce https in production
    if (process.env.NODE_ENV === 'production') {
      req.headers['x-forwarded-proto'] = 'https'
    }

    id = Buffer.from(id, 'utf-8').toString('base64url')
    return super.generateUrl(req, id)
  }

  getFileIdFromRequest(rawRwq: http.IncomingMessage): string | false {
    const req = rawRwq as MultiPartRequest
    const match = reExtractFileID.exec(req.url as string)

    if (!match || this.genOptions.path.includes(match[1])) {
      return false
    }

    const idMatch = Buffer.from(match[1], 'base64url').toString('utf-8')
    return req.upload.tenantId + '/' + idMatch
  }
}
