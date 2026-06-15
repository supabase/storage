import { ErrorCode } from '@internal/errors'
import { DBError } from '@storage/database/errors'
import type { FastifyReply, FastifyRequest } from 'fastify'
import { DatabaseError } from 'pg'
import { vi } from 'vitest'
import { s3ErrorHandler } from './error-handler'

describe('s3ErrorHandler', () => {
  it('maps wrapped database slowdown errors to 429', () => {
    const request = createRequest('/s3/public/object')
    const reply = createReply()

    s3ErrorHandler(
      DBError.fromDBError(createPgError('08P01', 'no more connections allowed (max_client_conn)')),
      request,
      reply
    )

    expect(reply.status).toHaveBeenCalledWith(429)
    expect(reply.send).toHaveBeenCalledWith({
      Error: {
        Resource: 'public/object',
        Code: ErrorCode.SlowDown,
        Message: 'Too many connections issued to the database',
      },
    })
  })

  it('keeps wrapped non-slowdown connection errors as database errors', () => {
    const request = createRequest('/s3/public/object')
    const reply = createReply()

    s3ErrorHandler(
      DBError.fromDBError(createPgError('08P01', 'received invalid response: 58')),
      request,
      reply
    )

    expect(reply.status).toHaveBeenCalledWith(500)
    expect(reply.send).toHaveBeenCalledWith({
      Error: {
        Resource: 'public/object',
        Code: ErrorCode.DatabaseError,
        Message: 'database error, code: 08P01',
      },
    })
  })
})

function createPgError(code: string, message: string): DatabaseError {
  const error = new DatabaseError(message, message.length, 'error')
  error.code = code
  return error
}

function createRequest(url: string): FastifyRequest {
  return { url } as FastifyRequest
}

function createReply(): FastifyReply {
  const reply = {
    status: vi.fn(),
    send: vi.fn(),
  }
  reply.status.mockReturnValue(reply)
  reply.send.mockReturnValue(reply)
  return reply as unknown as FastifyReply
}
