import { afterEach, describe, expect, it, vi } from 'vitest'

const ENV = { ...process.env }

afterEach(() => {
  process.env = { ...ENV }
  vi.doUnmock('../internal/database/tenant')
  vi.resetModules()
})

describe('enforceDeleteObjectsLimit', () => {
  it('does not enforce the object request cap until hard limits are enabled', async () => {
    process.env.MULTI_TENANT = 'false'
    process.env.REQUEST_HARD_LIMITS_ENABLED = 'false'
    vi.resetModules()

    const { enforceDeleteObjectsLimit, MAX_OBJECTS_PER_REQUEST } = await import('./limits')

    await expect(
      enforceDeleteObjectsLimit('tenant-id', MAX_OBJECTS_PER_REQUEST + 1)
    ).resolves.toBeUndefined()
  })

  it('enforces the default object request cap when hard limits are enabled', async () => {
    process.env.MULTI_TENANT = 'false'
    process.env.REQUEST_HARD_LIMITS_ENABLED = 'true'
    vi.resetModules()

    const { enforceDeleteObjectsLimit, MAX_OBJECTS_PER_REQUEST } = await import('./limits')

    await expect(
      enforceDeleteObjectsLimit('tenant-id', MAX_OBJECTS_PER_REQUEST + 1)
    ).rejects.toMatchObject({
      code: 'InvalidRequest',
      message: `Bulk object requests are limited to ${MAX_OBJECTS_PER_REQUEST} objects per request.`,
    })
  })

  it('uses the tenant delete objects limit in multitenant mode', async () => {
    process.env.MULTI_TENANT = 'true'
    process.env.REQUEST_HARD_LIMITS_ENABLED = 'true'
    const getDeleteObjectsLimit = vi.fn().mockResolvedValue(2000)
    vi.doMock('../internal/database/tenant', () => ({
      getDeleteObjectsLimit,
      getFeatures: vi.fn(),
      getFileSizeLimit: vi.fn(),
    }))
    vi.resetModules()

    const { enforceDeleteObjectsLimit } = await import('./limits')

    await expect(enforceDeleteObjectsLimit('tenant-id', 1500)).resolves.toBeUndefined()
    await expect(enforceDeleteObjectsLimit('tenant-id', 2001)).rejects.toMatchObject({
      code: 'InvalidRequest',
      message: 'Bulk object requests are limited to 2000 objects per request.',
    })
    expect(getDeleteObjectsLimit).toHaveBeenCalledWith('tenant-id')
  })
})

describe('isValidKey — backwards compatibility with master', () => {
  it.each([
    ['a typical object path', 'folder/file-name_01.jpg'],
    ['every accepted punctuation character', "file/!-*'() &$=@;:+,?name"],
    ['underscore from the word-character set', 'file_name'],
    ['a single slash', '/'],
    ['a 1024-character key (new upper bound)', `${'a'.repeat(1023)}/`],
  ])('accepts %s', async (_name, key) => {
    const { isValidKey } = await import('./limits')
    expect(isValidKey(key)).toBe(true)
  })

  it.each([
    ['an empty string', ''],
    ['a tab (ASCII control)', 'file\tname'],
    ['a newline (ASCII control)', 'file\nname'],
    ['DEL (0x7F)', `file${String.fromCharCode(0x7f)}`],
    ['null byte', 'test\x00.txt'],
    ['hash (#)', 'file#.txt'],
    ['left bracket', 'file[.txt'],
    ['backslash', 'file\\.txt'],
    ['pipe', 'file|.txt'],
    ['percent', 'file%.txt'],
    ['UTF-8 char outside the legacy charset — Chinese', '文档.txt'],
    ['UTF-8 char outside the legacy charset — emoji', '😀.txt'],
    ['UTF-8 char outside the legacy charset — Arabic', 'ملف.pdf'],
  ])('rejects %s', async (_name, key) => {
    const { isValidKey } = await import('./limits')
    expect(isValidKey(key)).toBe(false)
  })
})

describe('isValidKey — byte-length ceiling (new in this PR)', () => {
  it('exports MAX_OBJECT_KEY_BYTES equal to S3 limit (1024)', async () => {
    const { MAX_OBJECT_KEY_BYTES } = await import('./limits')
    expect(MAX_OBJECT_KEY_BYTES).toBe(1024)
  })

  it('accepts a key exactly at the 1024-byte limit', async () => {
    const { isValidKey } = await import('./limits')
    expect(isValidKey('a'.repeat(1024))).toBe(true)
  })

  it('accepts a key one byte below the limit', async () => {
    const { isValidKey } = await import('./limits')
    expect(isValidKey('a'.repeat(1023))).toBe(true)
  })

  it('rejects a key one byte over the limit', async () => {
    const { isValidKey } = await import('./limits')
    expect(isValidKey('a'.repeat(1025))).toBe(false)
  })

  it('rejects a pathologically long key', async () => {
    const { isValidKey } = await import('./limits')
    expect(isValidKey('a'.repeat(10_000))).toBe(false)
  })
})

describe('isValidKey — path-traversal rejection (new in this PR)', () => {
  it.each([
    ['bare single dot', '.'],
    ['bare double-dot', '..'],
    ['double-dot at start', '../etc/passwd'],
    ['double-dot in middle', 'safe/../etc/passwd'],
    ['double-dot at end', 'foo/..'],
    ['single-dot segment at start', './file.txt'],
    ['single-dot segment in middle', 'foo/./bar'],
    ['single-dot segment at end', 'foo/.'],
  ])('rejects %s', async (_name, key) => {
    const { isValidKey } = await import('./limits')
    expect(isValidKey(key)).toBe(false)
  })

  it.each([
    ['a dot inside a filename (not a traversal segment)', 'file.txt'],
    ['a dot at end of filename', 'folder/file.txt'],
    ['multiple dots in filename', 'archive.tar.gz'],
    ['dot followed by non-slash non-dot', 'foo/.hidden/bar.txt'],
  ])('accepts %s (dot is part of a filename, not a segment)', async (_name, key) => {
    const { isValidKey } = await import('./limits')
    expect(isValidKey(key)).toBe(true)
  })
})

describe('normalizeObjectKey — NFC helper (new in this PR, unwired)', () => {
  it('collapses NFD to NFC for combining accents', async () => {
    const { normalizeObjectKey } = await import('./limits')
    // 'café' NFC = c + a + f + é (U+00E9) = 5 bytes utf-8
    // 'café' NFD = c + a + f + e + combining-acute (U+0301) = 6 bytes utf-8
    const nfc = 'café'
    const nfd = 'café'
    expect(normalizeObjectKey(nfd)).toBe(nfc)
    expect(normalizeObjectKey(nfc)).toBe(nfc)
  })

  it('is idempotent for every input', async () => {
    const { normalizeObjectKey } = await import('./limits')
    const samples = ['hello.txt', '😀.png', 'café.doc', '']
    for (const s of samples) {
      expect(normalizeObjectKey(normalizeObjectKey(s))).toBe(normalizeObjectKey(s))
    }
  })

  it('leaves ASCII keys byte-identical', async () => {
    const { normalizeObjectKey } = await import('./limits')
    const ascii = 'folder/file-name_01.jpg'
    expect(normalizeObjectKey(ascii)).toBe(ascii)
  })

  it('leaves emoji byte-identical (no NFC change)', async () => {
    const { normalizeObjectKey } = await import('./limits')
    const emoji = '\u{1F680}.png'
    expect(normalizeObjectKey(emoji)).toBe(emoji)
  })
})

describe('isValidBucketName', () => {
  it.each([
    ['a typical bucket name', 'my-bucket'],
    ['allowed punctuation', "bucket!.*'()+,?-"],
    ['a 100-character name', 'a'.repeat(100)],
  ])('accepts %s', async (_name, bucketName) => {
    const { isValidBucketName } = await import('./limits')
    expect(isValidBucketName(bucketName)).toBe(true)
  })

  it.each([
    ['empty string', ''],
    ['a 101-character name', 'a'.repeat(101)],
    ['a slash (allowed in keys but not buckets)', 'my/bucket'],
    ['a UTF-8 char', '文件夹'],
  ])('rejects %s', async (_name, bucketName) => {
    const { isValidBucketName } = await import('./limits')
    expect(isValidBucketName(bucketName)).toBe(false)
  })
})

describe('parseFileSizeToBytes', () => {
  it('keeps every significant figure of the size', async () => {
    const { parseFileSizeToBytes } = await import('./limits')
    expect(parseFileSizeToBytes('1.5MB')).toBe(1_500_000)
    expect(parseFileSizeToBytes('2.25GB')).toBe(2_250_000_000)
  })

  it('accepts lowercase units', async () => {
    const { parseFileSizeToBytes } = await import('./limits')
    expect(parseFileSizeToBytes('1gb')).toBe(1_000_000_000)
    expect(parseFileSizeToBytes('500kb')).toBe(500_000)
  })

  it('rejects a size it cannot parse', async () => {
    const { parseFileSizeToBytes } = await import('./limits')
    expect(() => parseFileSizeToBytes('bad')).toThrow()
    expect(() => parseFileSizeToBytes('1TB')).toThrow()
  })
})
