import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { gzipSync } from 'node:zlib'
import { executePprofCommand, parsePprofCommand } from './pprof-client'

describe('parsePprofCommand', () => {
  const now = new Date('2026-07-13T14:00:00.000Z')

  it('parses capture commands', () => {
    expect(parsePprofCommand(['capture', 'profile', '--seconds', '30'])).toEqual({
      name: 'capture',
      target: 'profile',
      seconds: 30,
    })
    expect(parsePprofCommand(['capture', 'heap'])).toEqual({
      name: 'capture',
      target: 'heap',
      seconds: 30,
    })
    expect(
      parsePprofCommand(['capture', 'heap-snapshot', '--output', 'heap.heapsnapshot'])
    ).toEqual({
      name: 'capture',
      target: 'heap-snapshot',
      output: 'heap.heapsnapshot',
    })
  })

  it('parses stored profile commands', () => {
    expect(
      parsePprofCommand(['list', '--class', 'auto', '--kind', 'cpu', '--limit', '25'], now)
    ).toEqual({
      name: 'list',
      class: 'auto',
      kind: 'cpu',
      date: '2026-07-13',
      limit: 25,
      cursor: undefined,
      allPages: false,
      downloadDirectory: undefined,
      generateFlame: false,
    })
  })

  it('selects UTC profile dates', () => {
    expect(parsePprofCommand(['list', '--class', 'auto', '--days-ago', '1'], now)).toMatchObject({
      date: '2026-07-12',
    })
    expect(
      parsePprofCommand(['list', '--class', 'auto', '--date', '2026-07-01'], now)
    ).toMatchObject({ date: '2026-07-01' })
    expect(parsePprofCommand(['list', '--class', 'auto', '--all-dates'], now)).toMatchObject({
      date: undefined,
    })
  })

  it('parses list pagination and download options', () => {
    expect(
      parsePprofCommand(
        [
          'list',
          '--class',
          'manual',
          '--all-dates',
          '--all-pages',
          '--download',
          'profiles',
          '--flame',
        ],
        now
      )
    ).toMatchObject({
      name: 'list',
      class: 'manual',
      date: undefined,
      allPages: true,
      downloadDirectory: 'profiles',
      generateFlame: true,
    })
  })

  it('rejects invalid commands and options', () => {
    expect(() => parsePprofCommand(['profile'])).toThrow('Usage:')
    expect(() => parsePprofCommand(['capture', 'profile', '--seconds', '0'])).toThrow(
      'seconds must be a positive integer'
    )
    expect(() => parsePprofCommand(['capture', 'heap-snapshot', '--seconds', '10'])).toThrow(
      "Unknown option '--seconds'"
    )
    expect(() => parsePprofCommand(['capture', 'heap-snapshot', '--flame'])).toThrow(
      "Unknown option '--flame'"
    )
    expect(() => parsePprofCommand(['capture', 'profile', '--output', 'cpu.pb'])).toThrow(
      "Unknown option '--output'"
    )
    expect(() => parsePprofCommand(['capture', 'profile', '--flame'])).toThrow(
      "Unknown option '--flame'"
    )
    expect(() => parsePprofCommand(['capture', 'heap', '--output', 'heap.pb'])).toThrow(
      "Unknown option '--output'"
    )
    expect(() => parsePprofCommand(['capture', 'heap', '--flame'])).toThrow(
      "Unknown option '--flame'"
    )
    expect(() => parsePprofCommand(['list', '--class', 'automatic'])).toThrow(
      '--class must be auto or manual'
    )
    expect(() =>
      parsePprofCommand(['list', '--class', 'auto', '--date', '2026-02-30'], now)
    ).toThrow('date must use YYYY-MM-DD')
    expect(() =>
      parsePprofCommand(['list', '--class', 'auto', '--days-ago', '1', '--all-dates'], now)
    ).toThrow('mutually exclusive')
    expect(() => parsePprofCommand(['list', '--class', 'auto', '--all'], now)).toThrow(
      "Unknown option '--all'"
    )
    expect(() => parsePprofCommand(['list', '--class', 'auto', '--download='], now)).toThrow(
      'download directory must not be empty'
    )
    expect(() => parsePprofCommand(['list', '--class', 'auto', '--flame'], now)).toThrow(
      '--flame requires --download'
    )
    expect(() => parsePprofCommand(['download', 'profile-key'])).toThrow('Usage:')
  })
})

describe('executePprofCommand list', () => {
  let tempDir: string

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'pprof-client-list-'))
    vi.spyOn(console, 'log').mockImplementation(() => {})
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  it('follows every cursor and downloads every listed profile', async () => {
    const profiles = [
      {
        key: 'v1/auto/8215147911279-cc8065289c6e/cpu/d000010s_reason_host_a.storage_w.0_p.123_1.2.3.pprof.gz',
        class: 'auto' as const,
        kind: 'cpu' as const,
        reason: 'reason',
        startedAt: '2026-07-13T14:00:00.000Z',
        durationSeconds: 10,
        hostname: 'host',
        applicationId: 'storage',
        workerId: '0',
        processId: 123,
        build: '1.2.3',
      },
      {
        key: 'v1/auto/8215147910279-aabbccddeeff/heap/d000010s_reason_host_a.storage_w.1_p.123_1.2.3.pprof.gz',
        class: 'auto' as const,
        kind: 'heap' as const,
        reason: 'reason',
        startedAt: '2026-07-13T14:00:01.000Z',
        durationSeconds: 10,
        hostname: 'host',
        applicationId: 'storage',
        workerId: '1',
        processId: 123,
        build: '1.2.3',
      },
    ]
    const profile = gzipSync('profile-data')
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json({ profiles: [profiles[0]], cursor: 'next-page' }))
      .mockResolvedValueOnce(Response.json({ profiles: [profiles[1]] }))
      .mockResolvedValueOnce(
        new Response(profile, {
          headers: { 'content-disposition': 'attachment; filename="ignored.pprof.gz"' },
        })
      )
      .mockResolvedValueOnce(
        new Response(profile, {
          headers: { 'content-disposition': 'attachment; filename="ignored.pprof.gz"' },
        })
      )
    vi.stubGlobal('fetch', fetchMock)

    const result = await executePprofCommand(
      parsePprofCommand([
        'list',
        '--class',
        'auto',
        '--date',
        '2026-07-13',
        '--limit',
        '1',
        '--all-pages',
        '--download',
        tempDir,
      ]),
      'https://example.com/admin',
      'secret'
    )

    expect(result).toEqual({ profiles, cursor: undefined })
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      'https://example.com/admin/debug/pprof/profiles?class=auto&date=2026-07-13&limit=1',
      'https://example.com/admin/debug/pprof/profiles?class=auto&date=2026-07-13&limit=1&cursor=next-page',
      `https://example.com/admin/debug/pprof/profiles/download?key=${encodeURIComponent(profiles[0].key)}`,
      `https://example.com/admin/debug/pprof/profiles/download?key=${encodeURIComponent(profiles[1].key)}`,
    ])
    expect(await fs.readdir(tempDir)).toEqual([
      'auto-cpu-2026-07-13T14-00-00-000Z-cc8065289c6e.pprof.gz',
      'auto-heap-2026-07-13T14-00-01-000Z-aabbccddeeff.pprof.gz',
    ])
  })

  it('downloads only the returned page unless all pages are requested', async () => {
    const profile = {
      key: 'v1/manual/8215147911279-cc8065289c6e/cpu/d000010s_admin_host_a.storage_w.0_p.123_1.2.3.pprof.gz',
      class: 'manual' as const,
      kind: 'cpu' as const,
      reason: 'admin',
      startedAt: '2026-07-13T14:00:00.000Z',
      durationSeconds: 10,
      hostname: 'host',
      applicationId: 'storage',
      workerId: '0',
      processId: 123,
      build: '1.2.3',
    }
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json({ profiles: [profile], cursor: 'next-page' }))
      .mockResolvedValueOnce(new Response(gzipSync('profile-data')))
    vi.stubGlobal('fetch', fetchMock)

    const result = await executePprofCommand(
      parsePprofCommand([
        'list',
        '--class',
        'manual',
        '--date',
        '2026-07-13',
        '--download',
        tempDir,
      ]),
      'https://example.com/admin',
      'secret'
    )

    expect(result).toEqual({ profiles: [profile], cursor: 'next-page' })
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })
})
