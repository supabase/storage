import { IncomingMessage, ServerResponse } from 'node:http'
import { Socket } from 'node:net'
import { Readable } from 'node:stream'
import { GetObjectCommand, S3Client, S3ServiceException } from '@aws-sdk/client-s3'
import { ERRORS, ErrorCode } from '@internal/errors'
import { DBError } from '@storage/database/errors'
import type { FastifyReply, FastifyRequest } from 'fastify'
import { DatabaseError } from 'pg'
import { vi } from 'vitest'
import { formatS3ErrorResponse, s3ErrorHandler } from './error-handler'

describe('formatS3ErrorResponse resource normalization', () => {
  it('includes the current server time for clock skew errors', () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(new Date('2026-10-09T12:00:00Z'))
    try {
      expect(
        formatS3ErrorResponse(ERRORS.RequestTimeTooSkewed(), { url: '/s3/public/object' })
      ).toEqual({
        Error: {
          Resource: 'public/object',
          Code: 'RequestTimeTooSkewed',
          Message: "The difference between the request time and the server's time is too large.",
          ServerTime: '2026-10-09T12:00:00.000Z',
        },
      })
    } finally {
      vi.useRealTimers()
    }
  })

  it.each([
    ['strips the S3 route prefix', '/s3/public/object', 'public/object'],
    ['drops the query string', '/s3/public/object?uploads', 'public/object'],
    ['preserves nested s3 segments', '/s3/bucket/s3/key', 'bucket/s3/key'],
    ['normalizes a trailing slash', '/s3/public/object/', 'public/object'],
    ['returns an empty resource for the root path', '/s3', ''],
    ['returns an empty resource for the slash root path', '/s3/', ''],
    ['returns an empty resource for an empty URL', '', ''],
  ])('%s', (_case, url, expected) => {
    expect(
      formatS3ErrorResponse({ code: 'TestError', message: 'test error' }, { url }).Error.Resource
    ).toBe(expected)
  })
})

describe('s3ErrorHandler', () => {
  it('preserves Content-Range when an S3 byte range is unsatisfiable', async () => {
    const client = new S3Client({
      region: 'us-east-1',
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
      requestHandler: {
        handle: async () => ({
          response: {
            statusCode: 416,
            headers: {
              'content-type': 'application/xml',
              'content-range': 'bytes */10',
            },
            body: Readable.from([
              '<Error><Code>InvalidRange</Code><Message>Invalid range</Message></Error>',
            ]),
          },
        }),
      },
    })
    const request = createRequest('/s3/public/object')
    const reply = createReply(request)

    try {
      const error = await client
        .send(new GetObjectCommand({ Bucket: 'public', Key: 'object', Range: 'bytes=10-' }))
        .catch((error: unknown) => error)

      expect(error).toBeInstanceOf(S3ServiceException)
      s3ErrorHandler(error as S3ServiceException, request, reply)

      expect(reply.status).toHaveBeenCalledWith(416)
      expect(reply.header).toHaveBeenCalledWith('content-range', 'bytes */10')
      expect(reply.send).toHaveBeenCalledWith({
        Error: { Resource: 'public/object', Code: 'InvalidRange', Message: 'Invalid range' },
      })
    } finally {
      client.destroy()
      request.raw.destroy()
    }
  })

  it.each([
    [
      'the upstream XML code',
      412,
      '<Error><Code>PreconditionFailed</Code><Message>ETag changed</Message></Error>',
      'PreconditionFailed',
      'ETag changed',
      {},
    ],
    ['S3Error when the upstream response has no XML code', 403, '', 'S3Error', 'UnknownError', {}],
    [
      'the upstream clock skew code with Storage server time',
      403,
      '<Error><Code>RequestTimeTooSkewed</Code><Message>Clock differs</Message></Error>',
      'RequestTimeTooSkewed',
      'Clock differs',
      { ServerTime: expect.any(String) },
    ],
  ])('reports %s for an S3 SDK error', async (_name, statusCode, body, Code, Message, details) => {
    const client = new S3Client({
      region: 'us-east-1',
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
      requestHandler: {
        handle: async () => ({
          response: {
            statusCode,
            headers: { 'content-type': 'application/xml' },
            body: Readable.from(body ? [body] : []),
          },
        }),
      },
    })
    const request = createRequest('/s3/public/object')
    const reply = createReply(request)

    try {
      const error = await client
        .send(new GetObjectCommand({ Bucket: 'public', Key: 'object', IfMatch: '"stale"' }))
        .catch((error: unknown) => error)
      expect(error).toBeInstanceOf(S3ServiceException)
      s3ErrorHandler(error as S3ServiceException, request, reply)

      expect(reply.status).toHaveBeenCalledWith(statusCode)
      expect(reply.send).toHaveBeenCalledWith({
        Error: { Resource: 'public/object', Code, Message, ...details },
      })
    } finally {
      client.destroy()
      request.raw.destroy()
    }
  })

  it('records the error before clearing staged headers', () => {
    const request = createRequest('/s3/public/object')
    const reply = createReply(request)
    vi.spyOn(reply.raw, 'removeHeader').mockImplementation(() => {
      throw new Error('headers sent')
    })
    const error = ERRORS.InvalidRequest('rejected')

    try {
      expect(() => s3ErrorHandler(error, request, reply)).toThrow('headers sent')
      expect(request.executionError).toBe(error)
    } finally {
      request.raw.destroy()
    }
  })

  it('retains explicit-close errors for the response hook', () => {
    const request = createRequest('/s3/public/object')
    const reply = createReply(request)
    const error = ERRORS.InvalidRequest('rejected').withConnectionClose()

    try {
      s3ErrorHandler(error, request, reply)
      expect(request.executionError).toBe(error)
      expect(reply.status).toHaveBeenCalledWith(400)
    } finally {
      request.raw.destroy()
    }
  })

  it('maps wrapped database slowdown errors to 429', () => {
    const request = createRequest('/s3/public/object')
    const reply = createReply(request)

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
    const reply = createReply(request)

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
  return { url, raw: new IncomingMessage(new Socket()) } as FastifyRequest
}

function createReply(request: FastifyRequest): FastifyReply {
  const reply = {
    raw: new ServerResponse(request.raw),
    removeHeader: vi.fn(),
    getHeader: vi.fn(),
    getHeaders: () => reply.raw.getHeaders(),
    header: vi.fn(),
    status: vi.fn(),
    send: vi.fn(),
  }
  reply.status.mockReturnValue(reply)
  reply.send.mockReturnValue(reply)
  return reply as unknown as FastifyReply
}
