import { describe, expect, it } from 'vitest'
import buildApp from './app'
import { mergeConfig } from './config'
import { stripFiniteKeyword } from './http/finite'

describe('public app', () => {
  it('registers shared Blob response handling', async () => {
    const app = buildApp()

    try {
      await app.ready()

      expect(app.hasPlugin('blob-response')).toBe(true)
    } finally {
      await app.close()
    }
  })

  it('installs finite validation on the production Fastify instance', async () => {
    const app = buildApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/render/image/public/avatars/cat.png?width=1e999',
      })

      expect(response.statusCode).toBe(400)
      expect(response.json().message).toContain('finite')
    } finally {
      await app.close()
    }
  })

  it('does not expose the internal finite keyword in OpenAPI', async () => {
    const app = buildApp({ exposeDocs: true })

    try {
      await app.ready()

      const spec = app.swagger()
      expect(stripFiniteKeyword(spec)).toEqual(spec)

      const response = await app.inject({
        method: 'GET',
        url: '/render/image/public/avatars/cat.png?width=Infinity',
      })
      expect(response.statusCode).toBe(400)
      expect(response.json().message).toContain('finite')
    } finally {
      await app.close()
    }
  })

  it('documents the vector bucket CRUD routes in the generated OpenAPI spec', async () => {
    mergeConfig({ vectorEnabled: true })
    const app = buildApp({ exposeDocs: true })

    try {
      await app.ready()
      const spec = app.swagger()
      const paths = spec.paths!

      expect(paths['/vector/CreateVectorBucket']!.post).toMatchObject({
        operationId: 'vectorBucketCreate',
        responses: {
          200: { description: 'Successful response' },
          '4XX': {
            content: {
              'application/json': { schema: { $ref: '#/components/schemas/errorSchema' } },
            },
          },
        },
      })

      expect(paths['/vector/GetVectorBucket']!.post).toMatchObject({
        operationId: 'vectorBucketGet',
        responses: {
          200: {
            content: {
              'application/json': {
                schema: { $ref: '#/components/schemas/getVectorBucketResponse' },
              },
            },
          },
        },
      })

      expect(paths['/vector/ListVectorBuckets']!.post).toMatchObject({
        operationId: 'vectorBucketList',
        responses: {
          200: {
            content: {
              'application/json': {
                schema: { $ref: '#/components/schemas/listVectorBucketsResponse' },
              },
            },
          },
        },
      })

      expect(paths['/vector/DeleteVectorBucket']!.post).toMatchObject({
        operationId: 'vectorBucketDelete',
      })
    } finally {
      await app.close()
      mergeConfig({ vectorEnabled: false })
    }
  })
})
