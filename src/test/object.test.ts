'use strict'
import app from '../app'

test('index route returns a status of 200', async () => {
  const response = await app().inject({
    method: 'GET',
    url: '/bucket',
  })
  expect(response.statusCode).toBe(403)
})
