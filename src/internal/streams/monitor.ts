import { pipeline, Readable } from 'node:stream'
import { trace } from '@opentelemetry/api'
import { createByteCounterStream } from './byte-counter'
import { monitorStreamSpeed } from './stream-speed'

/**
 * Monitor readable streams by tracking their speed and bytes read
 * @param dataStream
 */
export function monitorStream(dataStream: Readable) {
  const speedMonitor = monitorStreamSpeed(dataStream)
  const byteCounter = createByteCounterStream()
  const span = trace.getActiveSpan()

  // Limit measures array to prevent unbounded growth during long uploads
  const MAX_MEASURES = 60
  let measures: object[] = []

  // Handle the 'speed' event to collect speed measurements
  speedMonitor.on('speed', (bps) => {
    measures.push({
      'stream.speed': bps,
      'stream.bytesRead': byteCounter.bytes,
      'stream.status': {
        paused: dataStream.isPaused(),
        closed: dataStream.closed,
      },
    })

    // Keep only the last MAX_MEASURES entries to bound memory usage
    if (measures.length > MAX_MEASURES) {
      measures = measures.slice(-MAX_MEASURES)
    }

    span?.setAttributes({
      stream: JSON.stringify(measures),
    })
  })

  speedMonitor.on('close', () => {
    measures = []
    span?.setAttributes({ uploadRead: byteCounter.bytes })
  })

  // Propagate consumer cancellation upstream as well as source errors downstream.
  // The returned stream exposes errors to its consumer.
  return pipeline(speedMonitor, byteCounter.transformStream, () => {})
}
