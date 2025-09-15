import { FastifyInstance } from 'fastify'
import { db, dbSuperUser, jwt, s3vector, signatureV4 } from '../../plugins'
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

const { dbServiceRole } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authenticated(fastify) {
    fastify.register(signatureV4, {
      service: SignatureV4Service.S3VECTORS,
      allowBodyHash: true,
      skipIfJwtToken: true,
    })

    fastify.register(jwt, {
      enforceJwtRoles: [dbServiceRole],
      skipIfAlreadyAuthenticated: true,
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
  })
}
