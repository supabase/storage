import { createHash } from 'crypto'
import { Readable } from 'stream'
import { buffer } from 'stream/consumers'
import { ContentMD5TransformStream } from './content-md5'

describe('ContentMD5TransformStream', () => {
  it('passes through bytes with a matching MD5', async () => {
    const payload = Buffer.from('payload')
    const digest = createHash('md5').update(payload).digest('base64')

    await expect(
      buffer(Readable.from([payload]).pipe(new ContentMD5TransformStream(digest)))
    ).resolves.toEqual(payload)
  })

  it('rejects a mismatched MD5', async () => {
    const payload = Buffer.from('payload')
    const wrongDigest = createHash('md5').update('different').digest('base64')

    await expect(
      buffer(Readable.from([payload]).pipe(new ContentMD5TransformStream(wrongDigest)))
    ).rejects.toMatchObject({ code: 'BadDigest', httpStatusCode: 400 })
  })

  it.each([
    'not-base64',
    Buffer.alloc(15).toString('base64'),
    '',
  ])('rejects invalid digests %s', (digest) => {
    expect(() => new ContentMD5TransformStream(digest)).toThrow(
      expect.objectContaining({ code: 'InvalidDigest' })
    )
  })
})
