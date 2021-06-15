import { FastifyInstance } from 'fastify'
import copyObject from './copyObject'
import createObject from './createObject'
import deleteObject from './deleteObject'
import deleteObjects from './deleteObjects'
import getObject from './getObject'
import getPublicObject from './getPublicObject'
import getSignedObject from './getSignedObject'
import getSignedURL from './getSignedURL'
import listObjects from './listObjects'
import moveObject from './moveObject'
import updateObject from './updateObject'
import upsertObject from './upsertObject'

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  copyObject(fastify)
  createObject(fastify)
  deleteObject(fastify)
  deleteObjects(fastify)
  getObject(fastify)
  getSignedObject(fastify)
  getPublicObject(fastify)
  getSignedURL(fastify)
  moveObject(fastify)
  updateObject(fastify)
  listObjects(fastify)
  upsertObject(fastify)
}
