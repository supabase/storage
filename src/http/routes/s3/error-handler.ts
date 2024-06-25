import { FastifyError } from '@fastify/error'
import { FastifyRequest } from 'fastify/types/request'
import { FastifyReply } from 'fastify/types/reply'
import { S3ServiceException } from '@aws-sdk/client-s3'
import { DatabaseError } from 'pg'
import { ErrorCode, StorageBackendError } from '@internal/errors'

export const s3ErrorHandler = (
  error: FastifyError | Error,
  request: FastifyRequest,
  reply: FastifyReply
) => {
  request.executionError = error

  const resource = request.url
    .split('?')[0]
    .replace('/s3', '')
    .split('/')
    .filter((e) => e)
    .join('/')

  if ('validation' in error) {
    return reply.status(400).send({
      Error: {
        Resource: resource,
        Code: ErrorCode.InvalidRequest,
        Message: formatValidationError(error.validation).message,
      },
    })
  }

  if (error instanceof S3ServiceException) {
    return reply.status(error.$metadata.httpStatusCode || 500).send({
      Error: {
        Resource: resource,
        Code: error.$response?.body.Code || ErrorCode.S3Error,
        Message: error.message,
      },
    })
  }

  // database error
  if (
    error instanceof DatabaseError &&
    [
      'Max client connections reached',
      'remaining connection slots are reserved for non-replication superuser connections',
      'no more connections allowed',
      'sorry, too many clients already',
      'server login has been failing, try again later',
    ].some((msg) => (error as DatabaseError).message.includes(msg))
  ) {
    return reply.status(429).send({
      Error: {
        Resource: resource,
        Code: ErrorCode.SlowDown,
        Message: 'Too many connections issued to the database',
      },
    })
  }

  if (error instanceof StorageBackendError) {
    return reply.status(error.httpStatusCode || 500).send({
      Error: {
        Resource: resource,
        Code: error.code,
        Message: error.message,
      },
    })
  }

  return reply.status(500).send({
    Error: {
      Resource: resource,
      Code: ErrorCode.InternalError,
      Message: 'Internal Server Error',
    },
  })
}

function formatValidationError(errors: any) {
  let text = ''
  const separator = ', '

  for (let i = 0; i !== errors.length; ++i) {
    const e = errors[i]
    const instancePath = (e.instancePath || '').replace(/^\//, '')
    text += instancePath.split('/').join(separator) + ' ' + e.message + separator
  }
  return new Error(text.slice(0, -separator.length))
}
