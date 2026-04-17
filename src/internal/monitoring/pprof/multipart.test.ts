import type { FastifyReply } from 'fastify'
import { vi } from 'vitest'

const waitMock = vi.hoisted(() => vi.fn())

vi.mock('node:timers/promises', () => ({
  setTimeout: waitMock,
}))

import { waitForMultipartPprofWindow } from './multipart'
import type { MultipartPprofWriter } from './types'

function createReply() {
  const reply = {
    raw: {
      destroyed: false,
      end: vi.fn(),
      socket: {
        setKeepAlive: vi.fn(),
      },
      writableEnded: false,
      write: vi.fn(),
      writeHead: vi.fn(),
    },
  }

  return reply
}

describe('waitForMultipartPprofWindow', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('stops sending heartbeat parts after the first write failure', async () => {
    const reply = createReply()
    const writer: MultipartPprofWriter = {
      boundary: 'pprof-test',
      close: vi.fn(),
      writeBinaryPart: vi.fn(),
      writeJsonPart: vi.fn().mockReturnValue(false),
    }
    let keepAliveCallback: (() => void) | undefined
    let cleared = false
    const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval').mockImplementation(() => {
      cleared = true
    })

    vi.spyOn(globalThis, 'setInterval').mockImplementation(((callback: () => void) => {
      keepAliveCallback = callback
      return {
        unref: vi.fn(),
      } as unknown as NodeJS.Timeout
    }) as typeof setInterval)

    waitMock.mockImplementation(async () => {
      keepAliveCallback?.()

      if (!cleared) {
        keepAliveCallback?.()
      }
    })

    await waitForMultipartPprofWindow(
      reply as unknown as FastifyReply,
      writer,
      6,
      new AbortController().signal
    )

    expect(reply.raw.socket.setKeepAlive).toHaveBeenCalledWith(true, 5000)
    expect(writer.writeJsonPart).toHaveBeenCalledTimes(1)
    expect(clearIntervalSpy).toHaveBeenCalled()
  })
})
