import { FastifyInstance } from 'fastify'
import {
  db,
  dbSuperUser,
  enforceJwtRole,
  jwt,
  requireTenantFeature,
  s3vector,
  signatureV4,
} from '../../plugins'
import { getConfig } from '../../../config'

import createVectorBucket from './create-bucket'
import deleteVectorBucket from './delete-bucket'
import listVectorBuckets from './list-buckets'
import getVectorBucket from './get-bucket'

import createVectorIndex from './create-index'
import deleteVectorIndex from './delete-index'
import listIndexes from './list-indexes'
import getIndex from './get-index'

import getVectors from './get-vectors'
import putVectors from './put-vectors'
import listVectors from './list-vectors'
import queryVectors from './query-vectors'
import deleteVectors from './delete-vectors'
import { SignatureV4Service } from '@storage/protocols/s3'
import { setErrorHandler } from '../../error-handler'

export default async function routes(fastify: FastifyInstance) {
  const { dbServiceRole, vectorEnabled, isMultitenant } = getConfig()

  if (!vectorEnabled && !isMultitenant) {
    return
  }

  fastify.register(async function authenticated(fastify) {
    if (!vectorEnabled && isMultitenant) {
      fastify.register(requireTenantFeature('vectorBuckets'))
    }

    fastify.register(signatureV4, {
      service: SignatureV4Service.S3VECTORS,
      allowBodyHash: true,
      skipIfJwtToken: true,
    })

    fastify.register(jwt, {
      skipIfAlreadyAuthenticated: true,
    })

    fastify.register(enforceJwtRole, {
      roles: [dbServiceRole],
    })

    fastify.register(dbSuperUser)
    fastify.register(s3vector)

    fastify.register(createVectorIndex)
    fastify.register(deleteVectorIndex)
    fastify.register(listIndexes)
    fastify.register(getIndex)

    fastify.register(createVectorBucket)
    fastify.register(deleteVectorBucket)
    fastify.register(listVectorBuckets)
    fastify.register(getVectorBucket)

    fastify.register(putVectors)
    fastify.register(queryVectors)
    fastify.register(deleteVectors)
    fastify.register(listVectors)
    fastify.register(getVectors)
    setErrorHandler(fastify, {
      respectStatusCode: true,
    })
  })
}
