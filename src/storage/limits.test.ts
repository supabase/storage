import { afterEach, describe, expect, it, vi } from 'vitest'

const ENV = { ...process.env }

afterEach(() => {
  process.env = { ...ENV }
  vi.doUnmock('../internal/database/tenant')
  vi.resetModules()
})

// Bucket names still follow the stricter S3 bucket-naming rules and remain
// ASCII-only. The legacy oracle is kept only for bucket names — object keys
// intentionally accept a wider (UTF-8) charset now.
const LEGACY_VALID_BUCKET_NAME = /^(\w|!|-|\.|\*|'|\(|\)| |&|\$|@|=|;|:|\+|,|\?)*$/

function legacyIsValidBucketName(bucketName: string): boolean {
  return (
    bucketName.length > 0 && bucketName.length < 101 && LEGACY_VALID_BUCKET_NAME.test(bucketName)
  )
}

function findCharsetMismatches(
  currentValidator: (value: string) => boolean,
  legacyValidator: (value: string) => boolean
): string[] {
  const mismatches: string[] = []

  for (let codeUnit = 0; codeUnit <= 0xffff; codeUnit++) {
    const value = `a${String.fromCharCode(codeUnit)}b`
    if (currentValidator(value) !== legacyValidator(value)) {
      mismatches.push(`U+${codeUnit.toString(16).toUpperCase().padStart(4, '0')}`)
    }
  }

  const astralValue = 'a\u{1F600}b'
  if (currentValidator(astralValue) !== legacyValidator(astralValue)) {
    mismatches.push('U+1F600')
  }

  return mismatches
}

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

describe('isValidKey', () => {
  const allowedPunctuation = "/!-*'() &$=@;:+,?"
  const typicalKey = 'folder/file-name_01.jpg'

  it.each([
    // --- ASCII (backwards compatible) ---
    ['a typical object path', typicalKey],
    ['every accepted punctuation character', `file${allowedPunctuation}name`],
    ['underscore from the word-character set', 'file_name'],
    ['a single slash', '/'],
    ['a 1024-character key', `${'a'.repeat(1023)}/`],

    // --- Non-Latin scripts (real-world filenames) ---
    ['a Chinese filename', '文档.txt'],
    ['an Arabic filename', 'ملف.pdf'],
    ['a Cyrillic filename', 'документ.doc'],
    ['a Japanese mixed-script filename', 'ドキュメント.png'],
    ['a Korean Hangul filename', '문서.hwp'],
    ['a Hebrew filename', 'מסמך.txt'],
    ['a Thai filename', 'เอกสาร.pdf'],
    ['a Devanagari filename', 'दस्तावेज़.doc'],

    // --- Emoji & astral plane ---
    ['a BMP emoji', '😀.txt'],
    ['an astral-plane emoji (U+1F680)', '🚀.png'],
    ['a ZWJ family sequence', '👨‍👩‍👧‍👦.jpg'],
    ['a regional-indicator flag pair', '🇺🇸.txt'],
    ['combining diacritics (U+0301)', 'café.txt'],

    // --- Astral character in the original test list — now accepted ---
    ['a raw multi-script name', 'ファイル-emoji-😀.txt'],
  ])('accepts %s', async (_name, key) => {
    const { isValidKey } = await import('./limits')

    expect(isValidKey(key)).toBe(true)
  })

  it.each([
    // --- ASCII controls ---
    ['an empty string', ''],
    ['a tab', 'file\tname'],
    ['a newline', 'file\nname'],
    ['DEL (0x7F)', `file${String.fromCharCode(0x7f)}`],
    ['unit separator (0x1F)', `test${String.fromCharCode(0x1f)}.txt`],
    ['null byte', 'test\x00.txt'],

    // --- S3 "characters to avoid" (each in isolation) ---
    ['hash (#)', 'file#.txt'],
    ['left bracket', 'file[.txt'],
    ['right bracket', 'file].txt'],
    ['left brace', 'file{.txt'],
    ['right brace', 'file}.txt'],
    ['caret', 'file^.txt'],
    ['backtick', 'file`.txt'],
    ['double quote', 'file".txt'],
    ['less-than', 'file<.txt'],
    ['greater-than', 'file>.txt'],
    ['backslash', 'file\\.txt'],
    ['pipe', 'file|.txt'],
    ['percent', 'file%.txt'],
    ['tilde', 'file~.txt'],
    ['S3 characters to avoid (all together)', 'file#[]{}^~`"<>\\|'],
    ['a percent-encoded fragment', 'file%20name'],

    // --- Invisible-glyph attacks ---
    ['zero-width space (U+200B) hidden in name', 'test​.txt'],
    ['right-to-left override (U+202E) spoofing', 'test‮exe.txt'],
    ['BOM prefix (U+FEFF)', '﻿test.txt'],
    ['right-to-left mark (U+200F)', 'test‏.txt'],
    ['line separator (U+2028)', 'test .txt'],
    ['first strong isolate (U+2068)', 'test⁨.txt'],
    ['valid chars with a single ZWSP', 'valid​name.txt'],

    // --- C1 controls (U+0080-U+009F) ---
    ['C1 control U+0080', `file${String.fromCodePoint(0x80)}.txt`],
    ['C1 control U+009F (application program cmd)', `file${String.fromCodePoint(0x9f)}.txt`],

    // --- Additional invisible-format chars (per depthfirst-app review) ---
    ['combining grapheme joiner (U+034F)', `file${String.fromCodePoint(0x34f)}.txt`],
    ['Arabic letter mark (U+061C)', `file${String.fromCodePoint(0x61c)}.txt`],
    ['word joiner (U+2060)', `file${String.fromCodePoint(0x2060)}.txt`],

    // --- Path traversal ---
    ['double-dot at start', '../etc/passwd'],
    ['double-dot in middle', 'safe/../etc/passwd'],
    ['double-dot at end', 'foo/..'],
    ['single-dot segment', './file.txt'],
    ['bare single dot', 'foo/./bar'],
    ['bare double-dot', '..'],
    ['bare single-dot', '.'],
  ])('rejects %s', async (_name, key) => {
    const { isValidKey } = await import('./limits')

    expect(isValidKey(key)).toBe(false)
  })

  it('accepts a UTF-8 key up to the 1024-byte S3 limit', async () => {
    const { isValidKey } = await import('./limits')

    // Exactly 1024 ASCII bytes = 1024 chars
    const asciiMax = 'a'.repeat(1024)
    expect(isValidKey(asciiMax)).toBe(true)

    // Exactly 1023 bytes is still fine
    expect(isValidKey('a'.repeat(1023))).toBe(true)
  })

  it('rejects a key that exceeds the 1024-byte S3 limit', async () => {
    const { isValidKey } = await import('./limits')

    // 1025 ASCII bytes
    expect(isValidKey('a'.repeat(1025))).toBe(false)

    // A Chinese char is 3 UTF-8 bytes; 342 chars = 1026 bytes
    expect(isValidKey('文'.repeat(342))).toBe(false)

    // But 341 Chinese chars = 1023 bytes → accepted
    expect(isValidKey('文'.repeat(341))).toBe(true)
  })

  it('exports MAX_OBJECT_KEY_BYTES for callers that need the limit', async () => {
    const { MAX_OBJECT_KEY_BYTES } = await import('./limits')

    expect(MAX_OBJECT_KEY_BYTES).toBe(1024)
  })
})

describe('isValidBucketName', () => {
  it('matches the legacy charset for every UTF-16 code unit and an astral character', async () => {
    const { isValidBucketName } = await import('./limits')

    expect(findCharsetMismatches(isValidBucketName, legacyIsValidBucketName)).toEqual([])
  })

  it('accepts a 100-character name and rejects 101 characters', async () => {
    const { isValidBucketName } = await import('./limits')

    expect(isValidBucketName('a'.repeat(100))).toBe(true)
    expect(isValidBucketName('a'.repeat(101))).toBe(false)
  })

  it('rejects a slash that would be valid in an object key', async () => {
    const { isValidBucketName, isValidKey } = await import('./limits')

    expect(isValidBucketName('folder/name')).toBe(false)
    expect(isValidKey('folder/name')).toBe(true)
  })
})

describe('parseFileSizeToBytes', () => {
  it('keeps every significant figure of the size', async () => {
    const { parseFileSizeToBytes } = await import('./limits')

    expect(parseFileSizeToBytes('1024MB')).toBe(1_024_000_000)
    expect(parseFileSizeToBytes('2048KB')).toBe(2_048_000)
    expect(parseFileSizeToBytes('1234B')).toBe(1234)
  })

  it('returns whole bytes for every two-decimal size', async () => {
    const { parseFileSizeToBytes } = await import('./limits')
    const bytesPerHundredth = { GB: 10_000_000, MB: 10_000, KB: 10 }

    for (let hundredths = 1; hundredths <= 9999; hundredths++) {
      const size = (hundredths / 100).toFixed(2)
      for (const [unit, bytes] of Object.entries(bytesPerHundredth)) {
        expect(parseFileSizeToBytes(`${size}${unit}`)).toBe(hundredths * bytes)
      }
    }
  })

  it('accepts lowercase units', async () => {
    const { parseFileSizeToBytes } = await import('./limits')

    expect(parseFileSizeToBytes('1.5gb')).toBe(1_500_000_000)
    expect(parseFileSizeToBytes('50mb')).toBe(50_000_000)
  })

  it('rejects a size it cannot parse', async () => {
    const { parseFileSizeToBytes } = await import('./limits')

    for (const size of ['', 'MB', '10', '-1MB', '1.MB', '10TB', '10 MB']) {
      expect(() => parseFileSizeToBytes(size)).toThrow('Invalid file size format')
    }
  })
})
