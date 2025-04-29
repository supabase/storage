import { createHook } from 'node:async_hooks'
import { trace } from '@opentelemetry/api'

const THRESHOLD_NS = 1e8 // 100ms

const cache = new Map<number, { type: string; start?: [number, number] }>()

function init(asyncId: number, type: string, triggerAsyncId: number, resource: any) {
  cache.set(asyncId, {
    type,
  })
}

function destroy(asyncId: number) {
  cache.delete(asyncId)
}

function before(asyncId: number) {
  const cached = cache.get(asyncId)

  if (!cached) {
    return
  }

  cache.set(asyncId, {
    ...cached,
    start: process.hrtime(),
  })
}

function after(asyncId: number) {
  const cached = cache.get(asyncId)

  if (!cached) {
    return
  }

  cache.delete(asyncId)

  if (!cached.start) {
    return
  }

  const diff = process.hrtime(cached.start)
  const diffNs = diff[0] * 1e9 + diff[1]

  if (diffNs > THRESHOLD_NS) {
    const time = diffNs / 1e6 // in ms

    const tracer = trace.getTracer('event-loop-monitor')
    const activeSpan = trace.getActiveSpan()

    const newSpan = tracer.startSpan('event-loop-blocked', {
      startTime: new Date(new Date().getTime() - time),
      attributes: {
        asyncType: cached.type,
        label: 'EventLoopMonitor',
      },
      links: activeSpan ? [{ context: activeSpan.spanContext() }] : undefined,
    })

    newSpan.end()
  }
}

export const eventLoopMonitor = () => {
  const hook = createHook({ init, before, after, destroy })

  return {
    enable: () => {
      console.log('🥸  Initializing event loop monitor')

      hook.enable()
    },
    disable: () => {
      console.log('🥸  Disabling event loop monitor')

      hook.disable()
    },
  }
}
