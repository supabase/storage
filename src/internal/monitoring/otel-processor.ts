import { SpanProcessor, ReadableSpan } from '@opentelemetry/sdk-trace-base'
import TTLCache from '@isaacs/ttlcache'

interface Trace {
  id: string
  rootSpanId: string
  spans: Span[]
}

export interface Span {
  item: ReadableSpan
  children: Span[]
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
    const traceId = span.spanContext().traceId
    const spanId = span.spanContext().spanId

    // No action needed on start
    if (!span.parentSpanId) {
      const hasTrace = this.traces.has(traceId)

      if (!hasTrace) {
        this.traces.set(traceId, {
          id: traceId,
          spans: [
            {
              item: span,
              children: [],
            },
          ],
          rootSpanId: spanId,
        })
      }

      return
    }

    const trace = this.traces.get(traceId)

    if (trace) {
      this.addChildSpan(trace.spans, span.parentSpanId, { item: span, children: [] })
    }
  }

  onEnd(span: ReadableSpan): void {
    // no-op
  }

  shutdown(): Promise<void> {
    this.traces.clear()
    return Promise.resolve()
  }

  forceFlush(): Promise<void> {
    this.traces.clear()
    return Promise.resolve()
  }

  getSpansForTrace(traceId: string): Span[] {
    return this.traces.get(traceId)?.spans || []
  }

  clearTrace(traceId: string): void {
    this.traces.delete(traceId)
  }

  private addChildSpan(spans: Span[], parentId: string, childSpan: Span): void {
    for (const span of spans) {
      if (span.item.spanContext().spanId === parentId) {
        span.children.push(childSpan)
        return
      }
      if (span.children.length > 0) {
        this.addChildSpan(span.children, parentId, childSpan)
      }
    }
  }
}

export const traceCollector = new TraceCollectorSpanProcessor()
