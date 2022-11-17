import { FastifyInstance } from 'fastify'
import { jwt, postgrest, superUserPostgrest, storage } from '../../plugins'
import copyObject from './copyObject'
import createObject from './createObject'
import deleteObject from './deleteObject'
import deleteObjects from './deleteObjects'
import getObject from './getObject'
import getPublicObject from './getPublicObject'
import getSignedObject from './getSignedObject'
import getSignedURL from './getSignedURL'
import getSignedURLs from './getSignedURLs'
import listObjects from './listObjects'
import moveObject from './moveObject'
import updateObject from './updateObject'
import {
  publicRoutes as getObjectInfoPublic,
  authenticatedRoutes as getObjectInfoAuth,
} from './getObjectInfo'

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authorizationContext(fastify) {
    fastify.register(jwt)
    fastify.register(postgrest)
    fastify.register(superUserPostgrest)
    fastify.register(storage)

    fastify.register(deleteObject)
    fastify.register(deleteObjects)
    fastify.register(getObject)
    fastify.register(getSignedURL)
    fastify.register(getSignedURLs)
    fastify.register(moveObject)
    fastify.register(updateObject)
    fastify.register(listObjects)
    fastify.register(getObjectInfoAuth)
    fastify.register(copyObject)
    fastify.register(createObject)
  })

  fastify.register(async (fastify) => {
    fastify.register(superUserPostgrest)
    fastify.register(storage)
    fastify.register(getPublicObject)
    fastify.register(getSignedObject)
    fastify.register(getObjectInfoPublic)
  })
}
