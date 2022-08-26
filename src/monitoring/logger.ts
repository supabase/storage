import pino from 'pino'
import { getConfig } from '../utils/config'

export const logger = pino({
  transport: buildTransport(),
  formatters: {
    level(label) {
      return { level: label }
    },
  },
  timestamp: pino.stdTimeFunctions.isoTime,
})

export function buildTransport(): pino.TransportSingleOptions | undefined {
  const { logflareApiKey, logflareSourceToken, logflareEnabled } = getConfig()

  if (!logflareEnabled) {
    return undefined
  }

  if (!logflareApiKey) {
    throw new Error('must provide a logflare api key')
  }

  if (!logflareSourceToken) {
    throw new Error('must provider a logflare source token')
  }

  return {
    target: './logflare',
  }
}
