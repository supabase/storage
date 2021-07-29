'use strict'
import app from '../app'
import dotenv from 'dotenv'

dotenv.config({ path: '.env.test' })

const ENV = process.env

beforeEach(() => {
  process.env = { ...ENV }
})

describe('with X-Forwarded-Host header', () => {
  test('PostgREST URL is constructed using X-Forwarded-Host if regexp matches', async () => {
    process.env.X_FORWARDED_HOST_REGEXP = '^[a-z]{20}\\.supabase\\.(co|in|net)$'
    const response = await app().inject({
      method: 'GET',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        'x-forwarded-host': 'abcdefghijklmnopqrst.supabase.co',
      },
    })
    expect(response.statusCode).toBe(500)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON.message).toContain('http://abcdefghijklmnopqrst.supabase.co/rest/v1')
  })

  test('PostgREST URL is not constructed using X-Forwarded-Host if regexp does not match', async () => {
    process.env.X_FORWARDED_HOST_REGEXP = '^[a-z]{20}\\.supabase\\.(co|in|net)$'
    const response = await app().inject({
      method: 'GET',
      url: `/bucket`,
      headers: {
        authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
        'x-forwarded-host': 'abcdefghijklmnopqrst.supabase.com',
      },
    })
    expect(response.statusCode).toBe(200)
    const responseJSON = JSON.parse(response.body)
    expect(responseJSON.length).toBe(5)
  })
})
