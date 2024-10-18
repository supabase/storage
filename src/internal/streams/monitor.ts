import { createByteCounterStream } from './byte-counter'
import { monitorStreamSpeed } from './stream-speed'
import { trace } from '@opentelemetry/api'
import { Readable } from 'node:stream'

/**
 * Monitor readable streams by tracking their speed and bytes read
 * @param dataStream
 */
export function monitorStream(dataStream: Readable) {
  const speedMonitor = monitorStreamSpeed(dataStream)
  const byteCounter = createByteCounterStream()

  let measures: number[] = []

  // Handle the 'speed' event to collect speed measurements
  speedMonitor.on('speed', (bps) => {
    measures.push(bps)
    const span = trace.getActiveSpan()
    span?.setAttributes({ 'stream.speed': measures, bytesRead: byteCounter.bytes })
  })

  speedMonitor.on('close', () => {
    measures = []
    const span = trace.getActiveSpan()
    span?.setAttributes({ uploadRead: byteCounter.bytes })
  })

  // Handle errors by cleaning up and destroying the downstream stream
  speedMonitor.on('error', (err) => {
    // Destroy the byte counter stream with the error
    byteCounter.transformStream.destroy(err)
  })

  // Ensure the byteCounter stream ends when speedMonitor ends
  speedMonitor.on('end', () => {
    byteCounter.transformStream.end()
  })

  // Return the piped stream
  return speedMonitor.pipe(byteCounter.transformStream)
}
