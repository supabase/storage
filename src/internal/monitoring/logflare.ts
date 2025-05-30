import dotenv from 'dotenv'
import pinoLogflare, { defaultPreparePayload } from 'pino-logflare'

export default async function () {
  dotenv.config()

  const logflareApiKey = process.env.LOGFLARE_API_KEY
  const logflareSourceToken = process.env.LOGFLARE_SOURCE_TOKEN
  const batchSizeEnv = process.env.LOGFLARE_BATCH_SIZE

  if (!logflareApiKey) {
    throw new Error('must provide a logflare api key')
  }

  if (!logflareSourceToken) {
    throw new Error('must provider a logflare source token')
  }

  return pinoLogflare({
    apiKey: logflareApiKey,
    apiBaseUrl: 'http://localhost:1337',
    sourceToken: logflareSourceToken,
    batchSize: 10, // batchSizeEnv ? parseInt(batchSizeEnv, 10) : 100,
    batchTimeout: 1000,
    onError: (_payload: any, err: Error) => {
      console.error(`[Logflare][Error] ${err.message} - ${err.stack}`)
    },
    onPreparePayload: (payload: any, meta: any) => {
      console.log('onPreparePayloadonPreparePayload', payload)
      const item = defaultPreparePayload(payload, meta)
      item.project = payload.project
      return item
    },
  } as any)
}
