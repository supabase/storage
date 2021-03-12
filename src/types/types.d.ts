import { RequestGenericInterface } from 'fastify'
import { FromSchema } from 'json-schema-to-ts'
import { objectSchema } from '../schemas/object'
import { bucketSchema } from '../schemas/bucket'

export type Bucket = FromSchema<typeof bucketSchema>
export type Obj = FromSchema<typeof objectSchema>

export type signedToken = {
  url: string
}

export interface AuthenticatedRequest extends RequestGenericInterface {
  Headers: {
    authorization: string
  }
}
type PostgrestError = {
  message: string
  details: string
  hint: string
  code: string
}

type StorageError = {
  statusCode: string
  error: string
  message: string
}
