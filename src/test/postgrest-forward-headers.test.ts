'use strict'
import dotenv from 'dotenv'
import * as pgjs from '@supabase/postgrest-js'
import fastify from 'fastify'
import { jwt, postgrest } from '../http/plugins'

dotenv.config({ path: '.env.test' })

beforeEach(() => {
  jest.clearAllMocks()
})

describe('Extra headers should be forwarded to postgrest client', () => {
  test('it should only preserve the headers that are defined in environment variable', async () => {
    process.env.POSTGREST_FORWARD_HEADERS = 'x-foo, x-bar'
    const extraHeaders = { 'x-foo': 1, 'x-bar': 2, 'x-none-exist': 3 }

    const pgSpy = jest.spyOn(pgjs, 'PostgrestClient')
    const app = fastify()
    app.register(jwt)
    app.register(postgrest)

    app.get('/test', (req, reply) => {
      console.log(req.jwt)
      reply.send('ok')
    })

    await app.inject({
      method: 'GET',
      url: `/test`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        ...extraHeaders,
      },
    })

    expect(pgSpy).toBeCalledTimes(1)
    expect(pgSpy).toHaveBeenCalledWith(process.env.POSTGREST_URL, {
      headers: {
        Authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        apiKey: process.env.ANON_KEY,
        'x-foo': '1',
        'x-bar': '2',
      },
      schema: 'storage',
    })
  })

  test('it should not preserve any extra headers when environment variable is not set', async () => {
    delete process.env.POSTGREST_FORWARD_HEADERS
    const extraHeaders = { 'x-foo': 1, 'x-bar': 2 }

    const pgSpy = jest.spyOn(pgjs, 'PostgrestClient')

    const app = fastify()
    app.register(jwt)
    app.register(postgrest)

    app.get('/test', (req, reply) => {
      console.log(req.jwt)
      reply.send('ok')
    })

    await app.inject({
      method: 'GET',
      url: `/test`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        ...extraHeaders,
      },
    })

    expect(pgSpy).toBeCalledWith(process.env.POSTGREST_URL, {
      headers: {
        Authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        apiKey: process.env.ANON_KEY,
      },
      schema: 'storage',
    })
  })
})
