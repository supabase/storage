import { FastifyError } from '@fastify/error'
import { FastifyRequest } from 'fastify/types/request'
import { FastifyReply } from 'fastify/types/reply'
import { S3ServiceException } from '@aws-sdk/client-s3'
import { ErrorCode, StorageBackendError } from '../../../storage'
import { DatabaseError } from 'pg'

export const s3ErrorHandler = (
  error: FastifyError | Error,
  request: FastifyRequest,
  reply: FastifyReply
) => {
  request.executionError = error

  console.log(error)

  const resource = request.url
    .split('?')[0]
    .replace('/s3', '')
    .split('/')
    .filter((e) => e)
    .join('/')

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
