'use strict'
import app from '../app'

import * as utils from '../utils/s3'

import { getConfig } from '../utils/config'
import dotenv from 'dotenv'
dotenv.config({ path: '.env.test' })
const { anonKey } = getConfig()

test('authenticated user is able to read authenticated resource', async () => {
  const mockGetObject = jest.spyOn(utils, 'getObject')
  mockGetObject.mockImplementation(() =>
    Promise.resolve({
      $metadata: {
        httpStatusCode: 200,
      },
      CacheControl: undefined,
      ContentDisposition: undefined,
      ContentEncoding: undefined,
      ContentLength: 3746,
      ContentType: 'image/png',
      Metadata: {},
    })
  )
  const response = await app().inject({
    method: 'GET',
    url: '/object/bucket2/authenticated/casestudy.png',
    headers: {
      authorization: `Bearer ${process.env.AUTHENTICATED_KEY}`,
    },
  })
  console.log(response)
  expect(response.statusCode).toBe(200)
})
