import { ErrorCode, StorageBackendError } from '@internal/errors'
import type { SignRequestOptions } from 'aws-sigv4-sign'
import JSONBigint from 'json-bigint'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const mockSignRequest = vi.fn()
vi.mock('aws-sigv4-sign', () => ({
  signRequest: (...args: unknown[]) => mockSignRequest(...args),
}))

import { IcebergError, IcebergErrorType } from './errors'
import {
  BearerTokenAuth,
  buildCatalogRequestUrl,
  RestCatalogClient,
  SignV4Auth,
} from './rest-catalog-client'

describe('RestCatalogClient request pipeline', () => {
  let fetchMock: ReturnType<typeof vi.fn>

  beforeEach(() => {
    fetchMock = vi.fn(
      async () =>
        new Response(JSON.stringify({ defaults: {} }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
    )
    vi.stubGlobal('fetch', fetchMock)

    mockSignRequest.mockReset()
    mockSignRequest.mockImplementation(
      async () =>
        new Request('https://signed.example.com/', {
          headers: { 'x-amz-date': '20200101T000000Z', authorization: 'AWS4-HMAC-SHA256 ...' },
        })
    )
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('preserves the catalogUrl path segment in the fetched URL', async () => {
    const client = new RestCatalogClient({
      catalogUrl: 'https://s3tables.ap-southeast-1.amazonaws.com/iceberg/v1',
      auth: new BearerTokenAuth({ token: 't' }),
    })

    await client.getConfig({ warehouse: 'wh' })

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const url = fetchMock.mock.calls[0][0] as string
    expect(url).toContain('/iceberg/v1/config')
  })

  it('preserves the catalogUrl path segment for nested resources', async () => {
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/some/prefix',
      auth: new BearerTokenAuth({ token: 't' }),
    })

    await client.loadTable({ warehouse: 'wh', namespace: 'ns', table: 'tbl' })

    const url = fetchMock.mock.calls[0][0] as string
    expect(url).toContain('/some/prefix/')
    expect(url).toMatch(/\/wh\/namespaces\/ns\/tables\/tbl/)
  })

  it('signs the exact URL that fetch is called with (SigV4)', async () => {
    const client = new RestCatalogClient({
      catalogUrl: 'https://s3tables.ap-southeast-1.amazonaws.com/iceberg/v1',
      auth: new SignV4Auth({ region: 'us-east-1' }),
    })

    await client.getConfig({ warehouse: 'my-warehouse' })

    expect(mockSignRequest).toHaveBeenCalledTimes(1)
    expect(fetchMock).toHaveBeenCalledTimes(1)

    const signedUrl = mockSignRequest.mock.calls[0][0] as string
    const fetchedUrl = fetchMock.mock.calls[0][0] as string

    expect(fetchedUrl).toBe(signedUrl)
  })

  it('produces identical signed and fetched params even when params include empty strings', async () => {
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new SignV4Auth({ region: 'us-east-1' }),
    })

    await client.listNamespaces({ warehouse: 'wh', pageToken: '', parent: 'p' })

    const signedUrl = mockSignRequest.mock.calls[0][0] as string
    const fetchedUrl = fetchMock.mock.calls[0][0] as string

    const signed = new URL(signedUrl)
    const fetched = new URL(fetchedUrl)

    expect(fetched.pathname).toBe(signed.pathname)
    expect(fetched.searchParams.toString()).toBe(signed.searchParams.toString())
    expect(fetched.searchParams.has('pageToken')).toBe(true)
    expect(fetched.searchParams.get('pageToken')).toBe('')
    expect(fetched.searchParams.get('parent')).toBe('p')
  })

  it('serializes array query params as repeated keys', () => {
    const url = buildCatalogRequestUrl('https://host.example.com/v1', '/wh/namespaces', {
      pageToken: ['one', 'two'],
    })

    expect(url.searchParams.getAll('pageToken')).toEqual(['one', 'two'])
  })

  it('encodes namespace and table path segments', async () => {
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 't' }),
    })

    await client.loadTable({
      warehouse: 'wh',
      namespace: 'ns/a b?#æ',
      table: 'tbl/a b?#æ',
    })

    const url = new URL(fetchMock.mock.calls[0][0] as string)
    expect(url.pathname).toBe(
      '/v1/wh/namespaces/ns%2Fa%20b%3F%23%C3%A6/tables/tbl%2Fa%20b%3F%23%C3%A6'
    )
  })

  it('signs the same URL for POST requests with a body', async () => {
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new SignV4Auth({ region: 'us-east-1' }),
    })

    await client.createNamespace({
      warehouse: 'wh',
      namespace: ['a', 'b'],
      properties: { k: 'v' },
    })

    const signedUrl = mockSignRequest.mock.calls[0][0] as string
    const fetchedUrl = fetchMock.mock.calls[0][0] as string
    expect(fetchedUrl).toBe(signedUrl)
  })

  it('sends bearer auth headers to fetch', async () => {
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 'catalog-token' }),
    })

    await client.getConfig({ warehouse: 'wh' })

    const init = fetchMock.mock.calls[0][1] as RequestInit
    expect(new Headers(init.headers).get('authorization')).toBe('Bearer catalog-token')
  })

  it('sends SigV4 headers to fetch', async () => {
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new SignV4Auth({ region: 'us-east-1' }),
    })

    await client.getConfig({ warehouse: 'wh' })

    const init = fetchMock.mock.calls[0][1] as RequestInit
    const headers = new Headers(init.headers)
    expect(headers.get('x-amz-date')).toBe('20200101T000000Z')
    expect(headers.get('authorization')).toBe('AWS4-HMAC-SHA256 ...')
  })

  it('can sign URL-encoded catalog requests with the real SigV4 signer', async () => {
    const actual = await vi.importActual<typeof import('aws-sigv4-sign')>('aws-sigv4-sign')
    mockSignRequest.mockImplementationOnce(
      (input: string | Request | URL, init: RequestInit, options: SignRequestOptions) =>
        actual.signRequest(input, init, options)
    )
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new SignV4Auth({
        region: 'us-east-1',
        credentials: {
          accessKeyId: 'AKIDEXAMPLE',
          secretAccessKey: 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY',
        },
      }),
    })

    await client.loadTable({
      warehouse: 'warehouse with space',
      namespace: 'ns/a b?#æ',
      table: 'tbl/a b?#æ',
    })

    const url = new URL(fetchMock.mock.calls[0][0] as string)
    expect(url.pathname).toBe(
      '/v1/warehouse%20with%20space/namespaces/ns%2Fa%20b%3F%23%C3%A6/tables/tbl%2Fa%20b%3F%23%C3%A6'
    )

    const init = fetchMock.mock.calls[0][1] as RequestInit
    const headers = new Headers(init.headers)
    expect(headers.get('x-amz-date')).toMatch(/^\d{8}T\d{6}Z$/)
    expect(headers.get('authorization')).toContain('AWS4-HMAC-SHA256')
    expect(headers.get('authorization')).toContain('SignedHeaders=')
  })

  it('passes a timeout signal to fetch', async () => {
    const controller = new AbortController()
    const timeout = vi.spyOn(AbortSignal, 'timeout').mockReturnValueOnce(controller.signal)
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 't' }),
      timeoutMs: 1234,
    })

    await client.getConfig({ warehouse: 'wh' })

    expect(timeout).toHaveBeenCalledWith(1234)
    const init = fetchMock.mock.calls[0][1] as RequestInit
    expect(init.signal).toBe(controller.signal)
  })

  it('serializes request bodies with JSONBigint', async () => {
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 't' }),
    })
    const largeFieldId = JSONBigint.parse('9223372036854775807')

    await client.createTable({
      warehouse: 'wh',
      namespace: 'ns',
      name: 'tbl',
      schema: {
        type: 'struct',
        fields: [
          {
            id: largeFieldId as number,
            name: 'id',
            type: 'long',
            required: true,
          },
        ],
      },
      spec: { fields: [] },
    })

    const init = fetchMock.mock.calls[0][1] as RequestInit
    expect(init.body).toContain('"id":9223372036854775807')
    expect(new Headers(init.headers).get('content-type')).toBe('application/json')
  })

  it('parses response bodies with JSONBigint', async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(
        '{"metadata-location":"s3://bucket/metadata.json","metadata":{"format-version":2,"table-uuid":"table-id","last-updated-ms":9223372036854775807}}',
        { status: 200, headers: { 'Content-Type': 'application/json' } }
      )
    )
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 't' }),
    })

    const result = await client.loadTable({ warehouse: 'wh', namespace: 'ns', table: 'tbl' })

    expect(String(result.metadata['last-updated-ms'])).toBe('9223372036854775807')
  })

  it('translates Iceberg JSON errors from non-2xx responses', async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          error: {
            message: 'table already exists',
            type: IcebergErrorType.AlreadyExistsException,
            code: 409,
          },
        }),
        { status: 409, headers: { 'Content-Type': 'application/json' } }
      )
    )
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 't' }),
    })

    await expect(
      client.createNamespace({ warehouse: 'wh', namespace: ['ns'] })
    ).rejects.toMatchObject({
      code: 409,
      message: 'table already exists',
      type: IcebergErrorType.AlreadyExistsException,
    } satisfies Partial<IcebergError>)
  })

  it('maps non-JSON error responses by HTTP status', async () => {
    fetchMock.mockResolvedValueOnce(new Response('missing', { status: 404 }))
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 't' }),
    })

    await expect(
      client.loadNamespaceMetadata({ warehouse: 'wh', namespace: 'ns' })
    ).rejects.toMatchObject({
      code: 404,
      message: 'Not found',
      type: IcebergErrorType.NoSuchNamespaceException,
    } satisfies Partial<IcebergError>)
  })

  it('maps empty 404 table responses to NoSuchTableException', async () => {
    fetchMock.mockResolvedValueOnce(new Response(null, { status: 404 }))
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 't' }),
    })

    await expect(
      client.tableExists({ warehouse: 'wh', namespace: 'ns', table: 'tbl' })
    ).rejects.toMatchObject({
      code: 404,
      message: 'Not found',
      type: IcebergErrorType.NoSuchTableException,
    } satisfies Partial<IcebergError>)
  })

  it('wraps network failures as internal storage errors', async () => {
    fetchMock.mockRejectedValueOnce(new TypeError('fetch failed'))
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 't' }),
    })

    await expect(client.getConfig({ warehouse: 'wh' })).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: 'Network error or Iceberg request failed',
      originalError: expect.any(TypeError),
    } satisfies Partial<StorageBackendError>)
  })

  it('wraps aborted requests while preserving the original AbortError', async () => {
    fetchMock.mockRejectedValueOnce(new DOMException('The operation was aborted', 'AbortError'))
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 't' }),
      timeoutMs: 1,
    })

    await expect(client.getConfig({ warehouse: 'wh' })).rejects.toMatchObject({
      code: ErrorCode.InternalError,
      message: 'Network error or Iceberg request failed',
      originalError: expect.objectContaining({ name: 'AbortError' }),
    } satisfies Partial<StorageBackendError>)
  })

  it('returns undefined for empty successful HEAD responses', async () => {
    fetchMock.mockResolvedValueOnce(new Response(null, { status: 204 }))
    const client = new RestCatalogClient({
      catalogUrl: 'https://host.example.com/v1',
      auth: new BearerTokenAuth({ token: 't' }),
    })

    await expect(
      client.tableExists({ warehouse: 'wh', namespace: 'ns', table: 'tbl' })
    ).resolves.toBeUndefined()

    const init = fetchMock.mock.calls[0][1] as RequestInit
    expect(init.method).toBe('HEAD')
  })
})
