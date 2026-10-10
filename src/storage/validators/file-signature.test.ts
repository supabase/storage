import { detectMimeFromBuffer, mimeFamiliesMatch, SIGNATURE_PEEK_BYTES } from './file-signature'

const hex = (s: string): Buffer => Buffer.from(s.replace(/\s+/g, ''), 'hex')

// Minimal real headers for the detectors we support. Each buffer is just the
// signature prefix padded with a few extra NULs so the detector sees "enough".
const SAMPLES = {
  jpeg:   hex('ffd8ffe000104a46494600'),
  png:    hex('89504e470d0a1a0a0000000d49484452'),
  gif87a: Buffer.from('GIF87a' + '\x00\x00\x00\x00', 'binary'),
  gif89a: Buffer.from('GIF89a' + '\x00\x00\x00\x00', 'binary'),
  webp:   Buffer.concat([Buffer.from('RIFF'), hex('18000000'), Buffer.from('WEBP')]),
  bmp:    Buffer.from('BM' + '\x00'.repeat(10), 'binary'),
  tiffLE: hex('49492a00'),
  tiffBE: hex('4d4d002a'),
  pdfBuf: Buffer.from('%PDF-1.4\n%EOF', 'binary'),
  mp4:    Buffer.concat([
    Buffer.from([0, 0, 0, 0x20]),      // box size
    Buffer.from('ftypmp42'),           // ftyp + brand
    Buffer.from([0, 0, 0, 0]),         // minor
    Buffer.from('isommp42'),           // compat brands
  ]),
  heic:   Buffer.concat([
    Buffer.from([0, 0, 0, 0x20]),
    Buffer.from('ftypheic'),
    Buffer.alloc(16),
  ]),
  avif:   Buffer.concat([
    Buffer.from([0, 0, 0, 0x20]),
    Buffer.from('ftypavif'),
    Buffer.alloc(16),
  ]),
  mp3:    Buffer.concat([Buffer.from('ID3'), Buffer.alloc(10)]),
  wav:    Buffer.concat([Buffer.from('RIFF'), hex('18000000'), Buffer.from('WAVE')]),
  ogg:    Buffer.from('OggS' + '\x00'.repeat(10), 'binary'),
  flac:   Buffer.from('fLaC' + '\x00'.repeat(10), 'binary'),
  webm:   Buffer.concat([hex('1a45dfa3'), Buffer.alloc(4)]),
  zip:    Buffer.concat([hex('504b0304'), Buffer.alloc(4)]),
  rar:    Buffer.concat([hex('526172211a0700'), Buffer.alloc(2)]),
  gzip:   Buffer.concat([hex('1f8b0800'), Buffer.alloc(4)]),
  exe:    Buffer.from('MZ' + '\x00'.repeat(60), 'binary'),
  elf:    Buffer.concat([hex('7f454c46'), Buffer.alloc(4)]),
  ole:    Buffer.concat([hex('d0cf11e0a1b11ae1'), Buffer.alloc(8)]),
}

describe('detectMimeFromBuffer', () => {
  it.each([
    [SAMPLES.jpeg,    'image/jpeg'],
    [SAMPLES.png,     'image/png'],
    [SAMPLES.gif87a,  'image/gif'],
    [SAMPLES.gif89a,  'image/gif'],
    [SAMPLES.webp,    'image/webp'],
    [SAMPLES.bmp,     'image/bmp'],
    [SAMPLES.tiffLE,  'image/tiff'],
    [SAMPLES.tiffBE,  'image/tiff'],
    [SAMPLES.pdfBuf,  'application/pdf'],
    [SAMPLES.mp4,     'video/mp4'],
    [SAMPLES.heic,    'image/heic'],
    [SAMPLES.avif,    'image/avif'],
    [SAMPLES.mp3,     'audio/mpeg'],
    [SAMPLES.wav,     'audio/wav'],
    [SAMPLES.ogg,     'audio/ogg'],
    [SAMPLES.flac,    'audio/flac'],
    [SAMPLES.webm,    'video/webm'],
    [SAMPLES.zip,     'application/zip'],
    [SAMPLES.rar,     'application/x-rar-compressed'],
    [SAMPLES.gzip,    'application/gzip'],
    [SAMPLES.exe,     'application/vnd.microsoft.portable-executable'],
    [SAMPLES.elf,     'application/x-elf'],
    [SAMPLES.ole,     'application/x-cfb'],
  ])('detects %#', (buf, expected) => {
    expect(detectMimeFromBuffer(buf as Buffer)).toBe(expected)
  })

  it('returns undefined for empty or tiny buffers', () => {
    expect(detectMimeFromBuffer(Buffer.alloc(0))).toBeUndefined()
    expect(detectMimeFromBuffer(Buffer.from([0xff]))).toBeUndefined()
  })

  it('returns undefined for unknown signatures', () => {
    // Plain ASCII text has no magic — detector must NOT guess.
    expect(detectMimeFromBuffer(Buffer.from('hello world'))).toBeUndefined()
    expect(detectMimeFromBuffer(hex('deadbeefcafe'))).toBeUndefined()
  })

  it('distinguishes RIFF/WEBP from RIFF/WAVE (same prefix)', () => {
    expect(detectMimeFromBuffer(SAMPLES.webp)).toBe('image/webp')
    expect(detectMimeFromBuffer(SAMPLES.wav)).toBe('audio/wav')
  })

  it('does NOT match RIFF with an unknown form chunk (defense against partial-prefix tricks)', () => {
    // RIFF without WEBP or WAVE suffix — may still match x-cfb if that entry has no refine? No,
    // x-cfb has a distinct 8-byte prefix. So the correct expectation is undefined.
    const riffUnknown = Buffer.concat([Buffer.from('RIFF'), hex('18000000'), Buffer.from('XXXX')])
    const detected = detectMimeFromBuffer(riffUnknown)
    expect(detected).toBeUndefined()
  })

  it('does NOT match ftyp with an unknown brand', () => {
    const ftypUnknown = Buffer.concat([
      Buffer.from([0, 0, 0, 0x20]),
      Buffer.from('ftypZZZZ'),
      Buffer.alloc(16),
    ])
    expect(detectMimeFromBuffer(ftypUnknown)).toBeUndefined()
  })

  it('SIGNATURE_PEEK_BYTES is at least 32 bytes (enough for every entry)', () => {
    expect(SIGNATURE_PEEK_BYTES).toBeGreaterThanOrEqual(32)
  })
})

describe('mimeFamiliesMatch', () => {
  it('accepts identical mimes (case + whitespace tolerant)', () => {
    expect(mimeFamiliesMatch('image/png', 'image/png')).toBe(true)
    expect(mimeFamiliesMatch('Image/PNG', 'image/png')).toBe(true)
    expect(mimeFamiliesMatch('image/jpeg; charset=binary', 'image/jpeg')).toBe(true)
  })

  it('treats image/jpg as an alias of image/jpeg', () => {
    expect(mimeFamiliesMatch('image/jpeg', 'image/jpg')).toBe(true)
    expect(mimeFamiliesMatch('image/jpg', 'image/jpeg')).toBe(true)
    expect(mimeFamiliesMatch('image/pjpeg', 'image/jpeg')).toBe(true)
  })

  it('accepts zip signature for OOXML office MIMEs (docx/xlsx/pptx)', () => {
    expect(
      mimeFamiliesMatch(
        'application/zip',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
      )
    ).toBe(true)
    expect(
      mimeFamiliesMatch(
        'application/zip',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      )
    ).toBe(true)
  })

  it('accepts x-cfb (OLE2) for legacy office MIMEs (doc/xls/ppt)', () => {
    expect(mimeFamiliesMatch('application/x-cfb', 'application/msword')).toBe(true)
    expect(mimeFamiliesMatch('application/x-cfb', 'application/vnd.ms-excel')).toBe(true)
    expect(mimeFamiliesMatch('application/x-cfb', 'application/vnd.ms-powerpoint')).toBe(true)
  })

  // The security-critical cases: a mismatch here means the uploader will reject.
  it.each([
    ['image/gif',  'image/jpeg'],  // renamed .gif → .jpg (exactly the #639 report)
    ['application/pdf', 'image/png'],
    ['application/vnd.microsoft.portable-executable', 'application/pdf'],
    ['image/jpeg', 'text/html'],   // polyglot JPG-HTML: declared text, actually JPG
    ['application/x-elf', 'image/jpeg'],
    ['application/zip',   'image/png'], // zip claiming to be an image
    ['application/gzip',  'image/jpeg'],
    ['video/mp4',  'image/jpeg'],
  ])('rejects mismatch detected=%s declared=%s', (detected, declared) => {
    expect(mimeFamiliesMatch(detected, declared)).toBe(false)
  })
})
