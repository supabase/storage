import { SpanProcessor, ReadableSpan } from '@opentelemetry/sdk-trace-base'
import TTLCache from '@isaacs/ttlcache'
import { getConfig } from '../../config'

const { isProduction, tracingTimeMinDuration } = getConfig()

interface Trace {
  id: string
  rootSpanId: string
  spans: ReadableSpan[]
}

export class TraceCollectorSpanProcessor implements SpanProcessor {
  private traces: TTLCache<string, Trace>

  constructor() {
    this.traces = new TTLCache<string, Trace>({
      ttl: 120 * 1000, // 120 seconds TTL
      noUpdateTTL: true,
      noDisposeOnSet: true,
    })
  }

  export() {
    // no-op
  }

  onStart(span: ReadableSpan): void {
    // No action needed on start
    if (!span.parentSpanId) {
      const traceId = span.spanContext().traceId
      const spanId = span.spanContext().spanId

      const hasTrace = this.traces.has(traceId)

      if (!hasTrace) {
        this.traces.set(traceId, {
          id: traceId,
          spans: [],
          rootSpanId: spanId,
        })
      }
    }
  }

  onEnd(span: ReadableSpan): void {
    const minLatency = isProduction ? tracingTimeMinDuration : 0.01

    // only add span if higher than min latency (no need to waste memory)
    if (span.duration[1] / 1e6 > minLatency) {
      const traceId = span.spanContext().traceId
      const trace = this.traces.get(traceId)

      if (!trace) {
        return
      }

      const cachedSpans = trace?.spans || []
      const whiteList = ['jwt', 'pg', 'raw', 's3', 'first', 'insert', 'select', 'delete']

      // only push top level spans
      if (whiteList.some((item) => span.name.toLowerCase().includes(item))) {
        cachedSpans.push(span)
        this.traces.set(traceId, {
          ...trace,
          spans: cachedSpans,
        })
      }
    }
  }

  shutdown(): Promise<void> {
    this.traces.clear()
    return Promise.resolve()
  }

  forceFlush(): Promise<void> {
    this.traces.clear()
    return Promise.resolve()
  }

  getSpansForTrace(traceId: string): ReadableSpan[] {
    return this.traces.get(traceId)?.spans || []
  }

  clearTrace(traceId: string): void {
    this.traces.delete(traceId)
  }
}

export const traceCollector = new TraceCollectorSpanProcessor()
