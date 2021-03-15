import { FastifyInstance } from 'fastify'
import search from './search'

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  search(fastify)
}
