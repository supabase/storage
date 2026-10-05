import { createServer } from 'node:http'
import type { Socket } from 'node:net'
import { gunzipSync } from 'node:zlib'
import type { ListObjectsV2Command, PutObjectCommand } from '@aws-sdk/client-s3'
import { S3Client } from '@aws-sdk/client-s3'
import { UndiciHttpHandler } from '@smithy/undici-http-handler'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  getGlobal: vi.fn(),
  profilingS3Endpoint: undefined as string | undefined,
}))

vi.mock('../../../config', () => ({
  getConfig: () => ({
    profilingS3Bucket: 'profiles',
    profilingS3Endpoint: mocks.profilingS3Endpoint,
    profilingS3ForcePathStyle: true,
    profilingS3Region: 'us-east-1',
    version: '1.2.3',
  }),
}))
vi.mock('@platformatic/globals', () => ({ getGlobal: mocks.getGlobal }))
vi.mock('os', () => ({ hostname: () => 'storage-host-a' }))

import {
  InvalidProfileCursorError,
  InvalidProfileDateError,
  ProfileNotFoundError,
  ProfileStore,
} from './store'
import { buildProfileKey, parseProfileKey } from './store-key'

const provenance = { applicationId: 'storage', workerId: '3' }

function createStore() {
  return new ProfileStore(new S3Client({ region: 'us-east-1' }), 'profiles')
}

describe('profile object keys', () => {
  beforeEach(() => mocks.getGlobal.mockReset())

  it('encodes Watt application and worker provenance', () => {
    const key = buildProfileKey(
      {
        class: 'auto',
        kind: 'cpu',
        reason: 'elu-sustained',
        startedAt: new Date('2026-07-12T14:30:12.123Z'),
        durationSeconds: 30,
      },
      provenance
    )

    expect(key).toMatch(
      /^v1\/auto\/\d{13}-[a-f0-9]{12}\/cpu\/d000030s_elu-sustained_storage-host-a_a\.storage_w\.3_p\.\d+_1\.2\.3\.pprof\.gz$/
    )
    expect(parseProfileKey(key)).toMatchObject({ key, applicationId: 'storage', workerId: '3' })
  })

  it('sorts newer captures first across kinds and dates', () => {
    mocks.getGlobal.mockReturnValue(undefined)
    const newer = buildProfileKey(
      {
        class: 'auto',
        kind: 'heap',
        reason: 'delay-severe',
        startedAt: new Date('2026-07-13T00:00:00.000Z'),
        durationSeconds: 30,
      },
      provenance
    )
    const older = buildProfileKey(
      {
        class: 'auto',
        kind: 'cpu',
        reason: 'elu-sustained',
        startedAt: new Date('2026-07-12T23:59:59.999Z'),
        durationSeconds: 30,
      },
      provenance
    )

    expect(newer < older).toBe(true)
  })

  it('normalizes long untrusted boundary runs without a backtracking trim', () => {
    mocks.getGlobal.mockReturnValue(undefined)
    const key = buildProfileKey(
      {
        class: 'auto',
        kind: 'cpu',
        reason: `${'-'.repeat(10_000)}admin event${'-'.repeat(10_000)}`,
        startedAt: new Date('2026-07-12T14:30:12.123Z'),
        durationSeconds: 30,
      },
      provenance
    )

    expect(key).toMatch(/_admin-event_storage-host-a_/)
  })

  it('preserves uploaded Watt provenance when listing', async () => {
    const store = createStore()
    const send = vi.spyOn(store.client, 'send').mockResolvedValue({} as never)

    try {
      await store.archive(
        {
          class: 'auto',
          kind: 'cpu',
          reason: 'elu-sustained',
          startedAt: new Date('2026-07-12T14:30:12.123Z'),
          durationSeconds: 30,
        },
        Buffer.from('profile'),
        provenance
      )
      const command = send.mock.calls[0][0] as PutObjectCommand
      const key = command.input.Key!
      expect(command.input).not.toHaveProperty('IfNoneMatch')
      expect(command.input).not.toHaveProperty('Metadata')
      expect(command.input.ContentType).toBe('application/gzip')
      const uploadedProfile = Buffer.from(command.input.Body as Uint8Array)
      expect([...uploadedProfile.subarray(0, 2)]).toEqual([0x1f, 0x8b])
      expect(gunzipSync(uploadedProfile).toString()).toBe('profile')

      send.mockResolvedValueOnce({ Contents: [{ Key: key }] } as never)
      const result = await store.list({ class: 'auto', limit: 1 })
      expect(result).toEqual({
        profiles: [
          expect.objectContaining({
            hostname: 'storage-host-a',
            applicationId: 'storage',
            workerId: '3',
            processId: process.pid,
            build: '1.2.3',
          }),
        ],
        cursor: undefined,
      })
      expect(result.profiles[0]).not.toHaveProperty('id')
    } finally {
      store.destroy()
    }
  })

  it('fills a filtered UTC-day page across S3 scan pages', async () => {
    mocks.getGlobal.mockReturnValue(undefined)
    const store = createStore()
    const key = (startedAt: string, kind: 'cpu' | 'heap' = 'cpu') =>
      buildProfileKey(
        {
          class: 'auto',
          kind,
          reason: 'elu-sustained',
          startedAt: new Date(startedAt),
          durationSeconds: 30,
        },
        provenance
      )
    const nextDay = key('2026-07-13T00:00:00.000Z')
    const wrongKind = key('2026-07-12T21:00:00.000Z', 'heap')
    const first = key('2026-07-12T20:00:00.000Z')
    const second = key('2026-07-12T19:00:00.000Z')
    const olderDay = key('2026-07-11T23:59:59.999Z')
    const send = vi
      .spyOn(store.client, 'send')
      .mockResolvedValueOnce({
        Contents: [{ Key: nextDay }, { Key: wrongKind }],
        IsTruncated: true,
      } as never)
      .mockResolvedValueOnce({
        Contents: [{ Key: first }, { Key: second }, { Key: olderDay }],
      } as never)

    try {
      const result = await store.list({
        class: 'auto',
        kind: 'cpu',
        date: '2026-07-12',
        limit: 2,
      })

      expect(result.profiles.map((profile) => profile.key)).toEqual([first, second])
      expect(Buffer.from(result.cursor!, 'base64url').toString('utf8')).toBe(second)
      expect(send).toHaveBeenCalledTimes(2)
      const firstCommand = send.mock.calls[0][0] as ListObjectsV2Command
      const secondCommand = send.mock.calls[1][0] as ListObjectsV2Command
      expect(firstCommand.input).toMatchObject({
        Prefix: 'v1/auto/',
        MaxKeys: 1000,
      })
      const reverseEnd = `${9_999_999_999_999 - Date.parse('2026-07-13T00:00:00.000Z')}`
      expect(firstCommand.input.StartAfter).toBe(`v1/auto/${reverseEnd}/`)
      expect(secondCommand.input.StartAfter).toBe(wrongKind)
    } finally {
      store.destroy()
    }
  })

  it('rejects impossible UTC profile dates before listing S3', async () => {
    const store = createStore()
    const send = vi.spyOn(store.client, 'send').mockResolvedValue({} as never)

    try {
      await expect(
        store.list({ class: 'auto', date: '2026-02-30', limit: 20 })
      ).rejects.toBeInstanceOf(InvalidProfileDateError)
      expect(send).not.toHaveBeenCalled()
    } finally {
      store.destroy()
    }
  })

  it('resumes listing after the last scanned key cursor', async () => {
    mocks.getGlobal.mockReturnValue(undefined)
    const store = createStore()
    const previousKey = buildProfileKey(
      {
        class: 'auto',
        kind: 'cpu',
        reason: 'elu-sustained',
        startedAt: new Date('2026-07-12T20:00:00.000Z'),
        durationSeconds: 30,
      },
      provenance
    )
    const send = vi.spyOn(store.client, 'send').mockResolvedValue({ Contents: [] } as never)

    try {
      await store.list({
        class: 'auto',
        cursor: Buffer.from(previousKey).toString('base64url'),
        limit: 20,
      })

      const command = send.mock.calls[0][0] as ListObjectsV2Command
      expect(command.input.StartAfter).toBe(previousKey)
      expect(command.input.MaxKeys).toBe(20)
    } finally {
      store.destroy()
    }
  })

  it('rejects a malformed cursor before listing S3', async () => {
    const store = createStore()
    const send = vi.spyOn(store.client, 'send')

    try {
      await expect(
        store.list({ class: 'auto', cursor: 'not+a+cursor', limit: 20 })
      ).rejects.toBeInstanceOf(InvalidProfileCursorError)
      expect(send).not.toHaveBeenCalled()
    } finally {
      store.destroy()
    }
  })

  it('maps invalid and missing profile keys to a not-found error', async () => {
    const store = createStore()
    const send = vi.spyOn(store.client, 'send')

    try {
      await expect(store.get('not-a-profile')).rejects.toBeInstanceOf(ProfileNotFoundError)
      expect(send).not.toHaveBeenCalled()

      const key = buildProfileKey(
        {
          class: 'auto',
          kind: 'cpu',
          reason: 'elu-sustained',
          startedAt: new Date('2026-07-12T14:30:12.123Z'),
          durationSeconds: 30,
        },
        provenance
      )
      send.mockRejectedValueOnce(Object.assign(new Error('missing'), { name: 'NotFound' }))

      await expect(store.get(key)).rejects.toBeInstanceOf(ProfileNotFoundError)
    } finally {
      store.destroy()
    }
  })

  it('preserves operational S3 failures', async () => {
    const store = createStore()
    const error = Object.assign(new Error('forbidden'), { name: 'AccessDenied' })
    vi.spyOn(store.client, 'send').mockRejectedValue(error)
    const key = buildProfileKey(
      {
        class: 'manual',
        kind: 'heap',
        reason: 'admin',
        startedAt: new Date('2026-07-12T14:30:12.123Z'),
        durationSeconds: 30,
      },
      provenance
    )

    try {
      await expect(store.get(key)).rejects.toBe(error)
    } finally {
      store.destroy()
    }
  })

  it('archives, lists and downloads over Undici and disposes its connections', async () => {
    let key = ''
    let archived = Buffer.alloc(0)
    const sockets = new Set<Socket>()
    const server = createServer(async (req, res) => {
      const url = new URL(req.url ?? '/', 'http://localhost')
      if (req.method === 'PUT') {
        key = url.pathname.slice('/profiles/'.length)
        const chunks: Buffer[] = []
        for await (const chunk of req) chunks.push(chunk)
        archived = Buffer.concat(chunks)
        res.writeHead(200, { etag: '"profile"' }).end()
      } else if (url.searchParams.has('list-type')) {
        res.setHeader('content-type', 'application/xml')
        res.end(
          `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>${key}</Key><Size>${archived.length}</Size></Contents></ListBucketResult>`
        )
      } else {
        res.writeHead(200, {
          'content-type': 'application/gzip',
          'content-length': archived.length,
        })
        res.end(archived)
      }
    })
    server.keepAliveTimeout = 60_000
    server.on('connection', (socket) => {
      sockets.add(socket)
      socket.once('close', () => sockets.delete(socket))
    })
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    let store: ProfileStore | undefined
    try {
      const address = server.address()
      if (!address || typeof address === 'string') throw new Error('missing S3 listener')
      vi.stubEnv('AWS_ACCESS_KEY_ID', 'test')
      vi.stubEnv('AWS_SECRET_ACCESS_KEY', 'test')
      mocks.profilingS3Endpoint = `http://127.0.0.1:${address.port}`
      vi.resetModules()
      const httpTransport = await import('@internal/http')
      const createAgent = vi.spyOn(httpTransport, 'createAgent')
      const { createProfileStore } = await import('./store')
      store = createProfileStore()
      expect(createAgent).toHaveBeenCalledWith('s3_profiling', {
        maxSockets: 50,
        requestTimeoutMs: 30_000,
      })
      expect(store.client.config.requestHandler).toBeInstanceOf(UndiciHttpHandler)
      await store.archive(
        {
          class: 'manual',
          kind: 'cpu',
          reason: 'admin',
          startedAt: new Date('2026-07-12T14:30:12.123Z'),
          durationSeconds: 30,
        },
        Buffer.from('profile'),
        provenance
      )
      const listed = await store.list({ class: 'manual', limit: 1 })
      expect(listed.profiles).toEqual([expect.objectContaining({ key, size: archived.length })])
      const { object } = await store.get(key)
      expect(gunzipSync(await object.Body!.transformToByteArray()).toString()).toBe('profile')
      expect(sockets.size).toBeGreaterThan(0)
      store.destroy()
      await vi.waitFor(() => expect(sockets.size).toBe(0))
    } finally {
      store?.destroy()
      mocks.profilingS3Endpoint = undefined
      server.closeAllConnections()
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})
