import { Readable } from 'stream'
import { PassThrough } from 'node:stream'

/**
 * Keep track of a stream's speed
 * @param stream
 * @param frequency
 */
/**
 * Keep track of a stream's speed
 * @param stream
 * @param frequency
 */
export function monitorStreamSpeed(stream: Readable, frequency = 1000) {
  let totalBytes = 0
  const startTime = Date.now()

  const passThrough = new PassThrough()

  const interval = setInterval(() => {
    const currentTime = Date.now()
    const elapsedTime = (currentTime - startTime) / 1000
    const currentSpeedBytesPerSecond = totalBytes / elapsedTime

    passThrough.emit('speed', currentSpeedBytesPerSecond)
  }, frequency)

  passThrough.on('data', (chunk) => {
    totalBytes += chunk.length
  })

  const cleanup = () => {
    clearInterval(interval)
    passThrough.removeAllListeners('speed')
  }

  // Handle close event to ensure cleanup
  passThrough.on('close', cleanup)

  // Propagate errors from the source stream to the passThrough
  stream.on('error', (err) => {
    passThrough.destroy(err)
  })

  // Ensure the passThrough ends when the source stream ends
  stream.on('end', () => {
    passThrough.end()
  })

  return stream.pipe(passThrough)
}
