'use strict'
import app from '../app'
import dotenv from 'dotenv'
import { PostgrestClient } from '@supabase/postgrest-js'
jest.mock('@supabase/postgrest-js')

dotenv.config({ path: '.env.test' })

beforeEach(() => {
  jest.clearAllMocks()
})

describe('Extra headers should be forwarded to postgrest client', () => {
  test('it should only preserve the headers that are defined in environment variable', async () => {
    process.env.POSTGREST_FORWARD_HEADERS = 'x-foo, x-bar'
    const extraHeaders = { 'x-foo': 1, 'x-bar': 2, 'x-none-exist': 3 }
    await app().inject({
      method: 'GET',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        ...extraHeaders,
      },
    })

    expect(PostgrestClient).toBeCalledWith(process.env.POSTGREST_URL, {
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

    await app().inject({
      method: 'GET',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        ...extraHeaders,
      },
    })

    expect(PostgrestClient).toBeCalledWith(process.env.POSTGREST_URL, {
      headers: {
        Authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        apiKey: process.env.ANON_KEY,
      },
      schema: 'storage',
    })
  })
})
