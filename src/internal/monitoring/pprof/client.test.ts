import {
  downloadArchivedProfile,
  fetchArchivedProfiles,
  fetchPprofStream,
  resolvePprofAdminUrl,
  triggerPprofCapture,
} from './client-http'

async function readStream(stream: NodeJS.ReadableStream) {
  const chunks: Buffer[] = []
  for await (const chunk of stream as AsyncIterable<Buffer | string | Uint8Array>) {
    chunks.push(Buffer.from(chunk))
  }
  return Buffer.concat(chunks).toString('utf8')
}

describe('pprof admin HTTP client', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('preserves ADMIN_URL path prefixes', () => {
    expect(
      resolvePprofAdminUrl('https://example.com/admin/internal', '/debug/pprof/profile', {
        seconds: 60,
      })
    ).toBe('https://example.com/admin/internal/debug/pprof/profile?seconds=60')
  })

  it.each([
    ['cpu', 'profile'],
    ['heap', 'heap'],
  ] as const)('triggers Watt manual %s captures for later download', async (type, path) => {
    const result = {
      scheduled: true as const,
      class: 'manual' as const,
      kind: type,
      message: 'Profile capture scheduled; use list and download to retrieve it',
    }
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValue(Response.json(result, { status: 202 }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      triggerPprofCapture({
        adminUrl: 'https://example.com/admin',
        apiKey: 'secret',
        type,
        seconds: 90,
      })
    ).resolves.toEqual(result)
    expect(fetchMock).toHaveBeenCalledWith(
      `https://example.com/admin/debug/pprof/${path}?seconds=90`,
      {
        headers: { Accept: 'application/json', ApiKey: 'secret' },
        method: 'GET',
        redirect: 'error',
      }
    )
  })

  it('requests JSON heap snapshots without a duration', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response('{}'))
    vi.stubGlobal('fetch', fetchMock)

    const response = await fetchPprofStream({
      adminUrl: 'https://example.com/admin',
      apiKey: 'secret',
      type: 'heap-snapshot',
    })

    const url = 'https://example.com/admin/debug/pprof/heap-snapshot'
    expect(fetchMock).toHaveBeenCalledWith(url, {
      headers: { Accept: 'application/json', ApiKey: 'secret' },
      method: 'GET',
      redirect: 'error',
    })
    expect(await readStream(response.stream)).toBe('{}')
  })

  it('lists and downloads stored profiles', async () => {
    const key = 'v1/auto/capture/cpu/profile.pprof.gz'
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json({ profiles: [], cursor: 'next' }))
      .mockResolvedValueOnce(new Response('stored-profile'))
    vi.stubGlobal('fetch', fetchMock)

    expect(
      await fetchArchivedProfiles({
        adminUrl: 'https://example.com/admin',
        apiKey: 'secret',
        class: 'auto',
        kind: 'cpu',
        date: '2026-07-13',
        limit: 20,
      })
    ).toEqual({ profiles: [], cursor: 'next' })
    expect(
      await readStream(
        (
          await downloadArchivedProfile({
            adminUrl: 'https://example.com/admin',
            apiKey: 'secret',
            key,
          })
        ).stream
      )
    ).toBe('stored-profile')

    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      'https://example.com/admin/debug/pprof/profiles?class=auto&kind=cpu&date=2026-07-13&limit=20',
      'https://example.com/admin/debug/pprof/profiles/download?key=v1%2Fauto%2Fcapture%2Fcpu%2Fprofile.pprof.gz',
    ])
    expect(fetchMock.mock.calls.map(([, init]) => init?.redirect)).toEqual(['error', 'error'])
  })

  it('caps error response bodies', async () => {
    vi.stubGlobal(
      'fetch',
      vi
        .fn<typeof fetch>()
        .mockResolvedValue(
          new Response('x'.repeat(6000), { status: 502, statusText: 'Bad Gateway' })
        )
    )

    await expect(
      fetchPprofStream({
        adminUrl: 'https://example.com/admin',
        apiKey: 'secret',
        type: 'heap-snapshot',
      })
    ).rejects.toThrow(/Pprof admin request failed: HTTP 502 Bad Gateway: .*\[truncated\]/)
  })

  it('cancels an error response whose first chunk exactly fills the limit', async () => {
    const cancel = vi.fn()
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(Buffer.alloc(4096, 'x'))
        controller.enqueue(Buffer.from('more'))
      },
      cancel,
    })
    vi.stubGlobal(
      'fetch',
      vi.fn<typeof fetch>().mockResolvedValue(new Response(body, { status: 502 }))
    )

    await expect(
      fetchPprofStream({
        adminUrl: 'https://example.com/admin',
        apiKey: 'secret',
        type: 'heap-snapshot',
      })
    ).rejects.toThrow(/\[truncated\]/)
    expect(cancel).toHaveBeenCalledOnce()
  })
})
