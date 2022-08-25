/* eslint-disable @typescript-eslint/no-var-requires */

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
  })
}
