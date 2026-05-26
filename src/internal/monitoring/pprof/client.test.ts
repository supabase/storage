import { fetchPprofStream, resolvePprofAdminUrl } from './client-http'

async function readStream(stream: NodeJS.ReadableStream) {
  const chunks: Buffer[] = []

  for await (const chunk of stream as AsyncIterable<Buffer | string | Uint8Array>) {
    if (typeof chunk === 'string') {
      chunks.push(Buffer.from(chunk))
      continue
    }

    chunks.push(Buffer.from(chunk))
  }

  return Buffer.concat(chunks).toString('utf8')
}

describe('resolvePprofAdminUrl', () => {
  it('preserves ADMIN_URL path prefixes when joining absolute-looking paths', () => {
    expect(
      resolvePprofAdminUrl('https://example.com/admin/internal', '/debug/pprof/profile', {
        seconds: 60,
        sourceMaps: false,
      })
    ).toBe('https://example.com/admin/internal/debug/pprof/profile?seconds=60&sourceMaps=false')

    expect(
      resolvePprofAdminUrl('https://example.com/admin/internal/', '/debug/pprof/heap', {
        workerId: 7,
      })
    ).toBe('https://example.com/admin/internal/debug/pprof/heap?workerId=7')
  })

  it('drops pre-existing query params from ADMIN_URL before adding request params', () => {
    expect(
      resolvePprofAdminUrl('https://example.com/admin/internal?stale=1', '/debug/pprof/profile', {
        seconds: 60,
      })
    ).toBe('https://example.com/admin/internal/debug/pprof/profile?seconds=60')
  })
})

describe('fetchPprofStream', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('requests multipart pprof output with the existing headers and query params', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response('profile-data', {
        headers: {
          'content-type': 'multipart/mixed; boundary=pprof-test',
        },
      })
    )
    vi.stubGlobal('fetch', fetchMock)

    const response = await fetchPprofStream({
      adminUrl: 'https://example.com/admin',
      apiKey: 'secret',
      nodeModulesSourceMaps: 'next,@next/next-server',
      seconds: 90,
      sourceMaps: true,
      type: 'profile',
      workerId: 0,
    })

    expect(fetchMock).toHaveBeenCalledWith(
      'https://example.com/admin/debug/pprof/profile?nodeModulesSourceMaps=next%2C%40next%2Fnext-server&seconds=90&sourceMaps=true&workerId=0',
      {
        headers: {
          Accept: 'multipart/mixed',
          ApiKey: 'secret',
        },
        method: 'GET',
      }
    )
    expect(response.contentType).toBe('multipart/mixed; boundary=pprof-test')
    expect(await readStream(response.stream)).toBe('profile-data')
  })

  it('surfaces non-2xx responses with the response body', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response('upstream failure', {
        status: 502,
        statusText: 'Bad Gateway',
      })
    )
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      fetchPprofStream({
        adminUrl: 'https://example.com/admin',
        apiKey: 'secret',
        seconds: 30,
        type: 'heap',
      })
    ).rejects.toThrow('Failed to capture pprof profile: HTTP 502 Bad Gateway: upstream failure')
  })

  it('caps verbose non-2xx response bodies', async () => {
    const noisyBody = 'x'.repeat(6000)
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(noisyBody, {
        status: 502,
        statusText: 'Bad Gateway',
      })
    )
    vi.stubGlobal('fetch', fetchMock)

    let error: unknown

    try {
      await fetchPprofStream({
        adminUrl: 'https://example.com/admin',
        apiKey: 'secret',
        seconds: 30,
        type: 'heap',
      })
    } catch (caught) {
      error = caught
    }

    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).toContain('… [truncated]')
    expect((error as Error).message.length).toBeLessThan(4300)
  })
})
