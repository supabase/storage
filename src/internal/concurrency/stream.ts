import { Transform, TransformCallback } from 'stream'

interface ByteCounterStreamOptions {
  maxHistory?: number
  onMaxHistory?: (history: Date[]) => void
  rewriteHistoryOnMax?: boolean
}

export const createByteCounterStream = (options: ByteCounterStreamOptions) => {
  const { maxHistory = 100 } = options

  let bytes = 0
  let history: Date[] = []

  const transformStream = new Transform({
    transform(chunk: Buffer, encoding: string, callback: TransformCallback) {
      bytes += chunk.length
      history.push(new Date())

      if (history.length === maxHistory) {
        if (options.rewriteHistoryOnMax) {
          options.onMaxHistory?.(history)
          history = []
        }
      }

      callback(null, chunk)
    },
  })

  return {
    transformStream,
    get bytes() {
      return bytes
    },
    get history() {
      return history
    },
  }
}
