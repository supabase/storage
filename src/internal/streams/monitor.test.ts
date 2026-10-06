import { PassThrough, Readable } from 'node:stream'
import { buffer } from 'node:stream/consumers'
import { setImmediate } from 'node:timers/promises'
import { S3Client } from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { monitorStream } from './monitor'
import { monitorStreamSpeed } from './stream-speed'

describe.each([
  ['upload monitor', monitorStream],
  ['speed monitor', monitorStreamSpeed],
] as const)('%s lifecycle', (_name, monitor) => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
  })

  afterEach(() => vi.useRealTimers())

  it('preserves every byte and clears its timer on successful completion', async () => {
    const chunks = [Buffer.from('object bytes'), Buffer.from([0, 255, 13, 10])]

    expect(await buffer(monitor(Readable.from(chunks)))).toEqual(Buffer.concat(chunks))
    await setImmediate()
    expect(vi.getTimerCount()).toBe(0)
  })

  it('stops the source and clears its timer when a consumer cancels', async () => {
    const source = new PassThrough()
    const output = monitor(source)
    expect(vi.getTimerCount()).toBe(1)

    output.destroy()
    await setImmediate()

    expect(source.destroyed).toBe(true)
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each([
    'source',
    'consumer',
  ] as const)('preserves a %s error while stopping the source and timer', async (side) => {
    const source = new PassThrough()
    const output = monitor(source)
    const failingStream = side === 'source' ? source : output
    const error = new Error(`upload ${side} failed`)
    const result = buffer(output)

    failingStream.destroy(error)
    await expect(result).rejects.toBe(error)
    await setImmediate()

    expect(source.errored).toBe(error)
    expect(vi.getTimerCount()).toBe(0)
  })
})

it('destroys the traced body when the S3 uploader rejects multipart creation', async () => {
  const client = new S3Client({ region: 'us-east-1' })
  vi.spyOn(client, 'send').mockRejectedValue(new Error('AccessDenied'))
  const source = new PassThrough()
  const upload = new Upload({
    client,
    partSize: 5 * 1024 * 1024,
    // A second uploader waiting on the shared chunk iterator would block its close.
    queueSize: 1,
    params: { Bucket: 'bucket', Key: 'key', Body: monitorStream(source) },
  })
  // Keep the body open after enough data to start the first multipart request.
  source.write(Buffer.alloc(6 * 1024 * 1024))

  await expect(upload.done()).rejects.toThrow('AccessDenied')
  await setImmediate()
  expect(source.destroyed).toBe(true)
})
