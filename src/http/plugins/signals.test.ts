import { EventEmitter } from 'node:events'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { describe, expect, it } from 'vitest'
import { RequestSignals } from './signals'

class FakeReq extends EventEmitter {
  aborted = false
}

class FakeRes extends EventEmitter {
  closed = false
  writableFinished = false
}

function build() {
  const req = new FakeReq()
  const res = new FakeRes()
  const signals = new RequestSignals(
    req as unknown as IncomingMessage,
    res as unknown as ServerResponse
  )
  return { req, res, signals }
}

describe('RequestSignals', () => {
  it('does not wire socket listeners until a signal is accessed', () => {
    const { req, res, signals } = build()

    expect(req.listenerCount('close')).toBe(0)
    expect(res.listenerCount('close')).toBe(0)

    // touch a signal
    void signals.body

    expect(req.listenerCount('close')).toBe(1)
    expect(res.listenerCount('close')).toBe(1)
  })

  it('wires the socket listeners only once across multiple accesses', () => {
    const { req, res, signals } = build()

    void signals.body
    void signals.response
    void signals.disconnect

    expect(req.listenerCount('close')).toBe(1)
    expect(res.listenerCount('close')).toBe(1)
  })

  it('returns the same controller instance on repeated access', () => {
    const { signals } = build()
    expect(signals.body).toBe(signals.body)
    expect(signals.disconnect).toBe(signals.disconnect)
  })

  it('aborts body and disconnect when the request closes after being aborted', () => {
    const { req, signals } = build()

    const body = signals.body
    const disconnect = signals.disconnect
    const response = signals.response

    req.aborted = true
    req.emit('close')

    expect(body.signal.aborted).toBe(true)
    expect(disconnect.signal.aborted).toBe(true)
    expect(response.signal.aborted).toBe(false)
  })

  it('does not abort body when the request closes cleanly', () => {
    const { req, signals } = build()

    const body = signals.body
    req.aborted = false
    req.emit('close')

    expect(body.signal.aborted).toBe(false)
  })

  it('aborts response and disconnect when the response closes before finishing', () => {
    const { res, signals } = build()

    const body = signals.body
    const response = signals.response
    const disconnect = signals.disconnect

    res.writableFinished = false
    res.emit('close')

    expect(response.signal.aborted).toBe(true)
    expect(disconnect.signal.aborted).toBe(true)
    expect(body.signal.aborted).toBe(false)
  })

  it('does not abort response when the response finishes normally', () => {
    const { res, signals } = build()

    const response = signals.response
    res.writableFinished = true
    res.emit('close')

    expect(response.signal.aborted).toBe(false)
  })

  it('aborts immediately when accessed after the request was already aborted', () => {
    const { req, signals } = build()
    req.aborted = true

    expect(signals.body.signal.aborted).toBe(true)
    expect(signals.disconnect.signal.aborted).toBe(true)
  })

  it('aborts immediately when accessed after the response was already aborted', () => {
    const { res, signals } = build()
    res.closed = true
    res.writableFinished = false

    expect(signals.response.signal.aborted).toBe(true)
    expect(signals.disconnect.signal.aborted).toBe(true)
  })

  it('abortRequest aborts only the controllers that were created', () => {
    const { signals } = build()

    // only body has been accessed
    const body = signals.body
    signals.abortRequest()

    expect(body.signal.aborted).toBe(true)
    // disconnect was never accessed; accessing it now should create a fresh,
    // non-aborted controller since the raw request was never actually aborted
    expect(signals.disconnect.signal.aborted).toBe(false)
  })
})
