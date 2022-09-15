'use strict'

import { generateForwardHeaders } from '../../utils/plugins'
import { FastifyRequest } from 'fastify'

describe('test generateForwardHeaders function', () => {
  test('it should return empty object when forwardHeaders is undefined', () => {
    const request: jest.Mocked<FastifyRequest> = { headers: { test: 'test' } } as any
    const result = generateForwardHeaders(undefined, request)
    expect(result).toStrictEqual({})
  })

  test('it should trim the forward headers and generate produce correct results', () => {
    const request: jest.Mocked<FastifyRequest> = {
      headers: {
        'custom-first-header': 'first',
        'custom-second-header': 'second',
      },
    } as any
    const result = generateForwardHeaders(' custom-first-header ,   custom-second-header  ', request)
    expect(result).toStrictEqual({
      'custom-first-header': 'first',
      'custom-second-header': 'second',
    })
  })

  test('it should ignore specific headers to forward if specified', () => {
    const request: jest.Mocked<FastifyRequest> = {
      headers: {
        'custom-first-header': 'first',
        'custom-second-header': 'second',
        'custom-third-header': 'third',
      },
    } as any
    const result = generateForwardHeaders(
      'custom-first-header,custom-second-header,custom-third-header',
      request,
      ['custom-third-header'],
    )
    expect(result).toStrictEqual({
      'custom-first-header': 'first',
      'custom-second-header': 'second',
    })
  })

})
