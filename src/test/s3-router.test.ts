import { Router } from '../http/routes/s3/router'
import { S3ProtocolHandler } from '../storage/protocols/s3/s3-handler'

describe('S3 router query matching', () => {
  it('parses key-only query params with an undefined value', () => {
    const router = new Router()

    expect(router.parseQueryMatch('uploads')).toEqual({
      key: 'uploads',
      value: undefined,
    })
  })

  it('matches key-only query params when the property exists', () => {
    const router = new Router()

    router.post(
      '/:Bucket/*?uploads',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(
      router.matchRoute(route!, {
        query: { uploads: undefined },
        headers: {},
      })
    ).toBe(true)
  })

  it('matches valued query params when the value matches', () => {
    const router = new Router()

    router.get(
      '/:Bucket/*?list-type=2',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(
      router.matchRoute(route!, {
        query: { 'list-type': '2' },
        headers: {},
      })
    ).toBe(true)
  })

  it('does not match valued query params when the value differs', () => {
    const router = new Router()

    router.get(
      '/:Bucket/*?list-type=2',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(
      router.matchRoute(route!, {
        query: { 'list-type': '1' },
        headers: {},
      })
    ).toBe(false)
  })

  it('matches wildcard routes even when the request has query params', () => {
    const router = new Router()

    router.get(
      '/:Bucket/*',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(
      router.matchRoute(route!, {
        query: { uploads: undefined },
        headers: {},
      })
    ).toBe(true)
  })
})

describe('S3ProtocolHandler.parseMetadataHeaders', () => {
  it('returns only x-amz-meta headers without the prefix', () => {
    const handler = new S3ProtocolHandler({} as any, 'tenant-id')

    expect(
      handler.parseMetadataHeaders({
        'content-type': 'application/json',
        'x-amz-meta-color': 'blue',
        'x-amz-meta-size': 'large',
      })
    ).toEqual({
      color: 'blue',
      size: 'large',
    })
  })

  it('returns undefined when there are no metadata headers', () => {
    const handler = new S3ProtocolHandler({} as any, 'tenant-id')

    expect(
      handler.parseMetadataHeaders({
        authorization: 'token',
        'content-type': 'application/json',
      })
    ).toBeUndefined()
  })

  it('keeps only string metadata values', () => {
    const handler = new S3ProtocolHandler({} as any, 'tenant-id')

    expect(
      handler.parseMetadataHeaders({
        'x-amz-meta-color': 'blue',
        'x-amz-meta-count': 1,
        'x-amz-meta-enabled': true,
        'x-amz-meta-tags': ['a', 'b'],
        'x-amz-meta-config': { mode: 'fast' },
      })
    ).toEqual({
      color: 'blue',
    })
  })

  it('returns undefined when metadata headers are present but none are strings', () => {
    const handler = new S3ProtocolHandler({} as any, 'tenant-id')

    expect(
      handler.parseMetadataHeaders({
        'x-amz-meta-count': 1,
        'x-amz-meta-enabled': false,
        'x-amz-meta-tags': ['a', 'b'],
      })
    ).toBeUndefined()
  })
})
