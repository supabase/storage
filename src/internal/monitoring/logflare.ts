/* eslint-disable @typescript-eslint/no-var-requires */

import { defaultPreparePayload } from 'pino-logflare'

const dotenv = require('dotenv')
const { createWriteStream: createLogFlareWriteStream } = require('pino-logflare')

export default function () {
  dotenv.config()

  const logflareApiKey = process.env.LOGFLARE_API_KEY
  const logflareSourceToken = process.env.LOGFLARE_SOURCE_TOKEN

  if (!logflareApiKey) {
    throw new Error('must provide a logflare api key')
  }

  if (!logflareSourceToken) {
    throw new Error('must provider a logflare source token')
  }

  return createLogFlareWriteStream({
    apiKey: logflareApiKey,
    sourceToken: logflareSourceToken,
    size: 100,
    onError: (err: Error) => {
      console.error(`[Logflare][Error] ${err.message} - ${err.stack}`)
    },
    onPreparePayload: (payload: any, meta: any) => {
      const item = defaultPreparePayload(payload, meta)
      item.project = payload.project
      return item
    },
  })
}
