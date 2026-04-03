import { S3ServiceException } from '@aws-sdk/client-s3'
import { FastifyError } from '@fastify/error'
import { ErrorCode, StorageBackendError } from '@internal/errors'
import { FastifyReply } from 'fastify/types/reply'
import { FastifyRequest } from 'fastify/types/request'
import { DatabaseError } from 'pg'

type ValidationIssue = {
  instancePath?: string
  message?: string
}

export const s3ErrorHandler = (
  error: FastifyError | Error,
  request: FastifyRequest,
  reply: FastifyReply
) => {
  request.executionError = error
  const validation = getValidationIssues(error)

  const resource = request.url
    .split('?')[0]
    .replace('/s3', '')
    .split('/')
    .filter((e) => e)
    .join('/')

  if (validation) {
    return reply.status(400).send({
      Error: {
        Resource: resource,
        Code: ErrorCode.InvalidRequest,
        Message: formatValidationError(validation).message,
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

  const statusCode =
    'statusCode' in error && typeof error.statusCode === 'number' ? error.statusCode : undefined

  if (statusCode && statusCode >= 400 && statusCode < 500) {
    return reply.status(statusCode).send({
      Error: {
        Resource: resource,
        Code: ErrorCode.InvalidRequest,
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

function isValidationIssueArray(value: unknown): value is ValidationIssue[] {
  return Array.isArray(value)
}

function getValidationIssues(error: FastifyError | Error): ValidationIssue[] | undefined {
  if (!('validation' in error)) {
    return undefined
  }

  const value = error.validation
  return isValidationIssueArray(value) ? value : undefined
}

function formatValidationError(errors: readonly ValidationIssue[]) {
  let text = ''
  const separator = ', '

  for (let i = 0; i !== errors.length; ++i) {
    const e = errors[i]
    const instancePath = (e.instancePath || '').replace(/^\//, '')
    text += instancePath.split('/').join(separator) + ' ' + e.message + separator
  }
  return new Error(text.slice(0, -separator.length))
}
