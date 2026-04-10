import fastify, { FastifyInstance } from 'fastify'
import { xmlParser } from '../http/plugins/xml'

async function buildXmlApp(parseAsArray: string[] = []): Promise<FastifyInstance> {
  const app = fastify()

  await app.register(xmlParser, { parseAsArray })

  app.post('/xml', async (req) => {
    return { body: req.body }
  })

  app.get('/xml', async () => {
    return {
      ListBucketResult: {
        Name: 'test-bucket',
      },
    }
  })

  return app
}

describe('xmlParser plugin', () => {
  it('parses XML bodies and enforces configured array paths', async () => {
    const app = await buildXmlApp(['CompleteMultipartUpload.Part'])

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/xml',
        headers: {
          'content-type': 'application/xml',
          accept: 'application/json',
        },
        payload:
          '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag-1</ETag></Part></CompleteMultipartUpload>',
      })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({
        body: {
          CompleteMultipartUpload: {
            Part: [{ PartNumber: 1, ETag: 'etag-1' }],
          },
        },
      })
    } finally {
      await app.close()
    }
  })

  it('returns 400 for malformed XML payloads', async () => {
    const app = await buildXmlApp()

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/xml',
        headers: {
          'content-type': 'application/xml',
          accept: 'application/json',
        },
        payload: '<CompleteMultipartUpload><Part></CompleteMultipartUpload>',
      })

      expect(response.statusCode).toBe(400)
      expect(response.json().message).toContain('Invalid XML payload')
    } finally {
      await app.close()
    }
  })

  it('serializes response payloads as XML when requested', async () => {
    const app = await buildXmlApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/xml',
        headers: {
          accept: 'application/xml',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(response.headers['content-type']).toContain('application/xml')
      expect(response.payload).toContain(
        '<ListBucketResult><Name>test-bucket</Name></ListBucketResult>'
      )
    } finally {
      await app.close()
    }
  })
})
