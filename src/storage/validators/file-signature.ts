import { PassThrough, Readable } from 'stream'

// Magic-byte (file-signature) detection for upload MIME validation.
//
// Why this file exists
// --------------------
// `src/storage/validators/mime-type.ts` only compares the client-declared MIME
// string against the bucket's `allowed_mime_types` list. A caller that renames
// a `.gif` to `.jpg` and sends `Content-Type: image/jpeg` passes that check
// even though the actual bytes are a GIF (issue #639, user flogesell, 2024).
//
// This helper peeks the first chunk of the upload stream and derives a MIME
// from the signature bytes. The uploader can then reject uploads whose actual
// content does not match the restriction, while keeping buckets with no
// restrictions on the fast path.
//
// Design notes
// ------------
// 1. Pure helper — no imports from logger / config / database. Keeps it reusable
//    from any upload code path (standard / S3 / resumable) without pulling in
//    circular dependencies.
// 2. Returns `undefined` for signatures it cannot identify. The uploader
//    treats that as "not enough evidence" and does NOT reject on it; otherwise
//    legitimate novel formats would start failing. We only refuse when we are
//    certain the actual MIME is in a different family than the declared one.
// 3. 4 KiB peek is enough for every entry in SIGNATURES below while still
//    being cheap to buffer in a PassThrough.

/** Minimum bytes we want before attempting detection. */
export const SIGNATURE_PEEK_BYTES = 4096

type SignatureEntry = {
  mime: string
  /** Either a prefix (array of ints), or a `{ offset, bytes }` for formats with a prefix such as ISO BMFF. */
  signatures: Array<number[] | { offset: number; bytes: number[] }>
  /**
   * Optional post-match check. Used for container formats (e.g. ISO BMFF)
   * where the magic prefix is shared across many sub-types.
   */
  refine?: (buf: Buffer) => string | undefined
}

/**
 * ISO Base Media File Format major-brand lookup. Called when the ftyp box is
 * detected (`...ftyp` at offset 4). The brand lives at bytes 8..12.
 */
function refineIsoBmff(buf: Buffer): string | undefined {
  if (buf.length < 12) return undefined
  const brand = buf.subarray(8, 12).toString('ascii')
  // Covers the brands bucket owners actually restrict on.
  switch (brand) {
    case 'mp42':
    case 'mp41':
    case 'isom':
    case 'avc1':
    case 'iso2':
    case 'iso4':
    case 'iso5':
    case 'iso6':
    case 'dash':
      return 'video/mp4'
    case 'M4V ':
      return 'video/x-m4v'
    case 'M4A ':
      return 'audio/mp4'
    case 'qt  ':
      return 'video/quicktime'
    case '3gp4':
    case '3gp5':
    case '3gp6':
      return 'video/3gpp'
    case 'heic':
    case 'heix':
    case 'hevc':
      return 'image/heic'
    case 'heis':
    case 'hevm':
    case 'hevs':
    case 'mif1':
    case 'msf1':
      return 'image/heif'
    case 'avif':
    case 'avis':
      return 'image/avif'
    default:
      return undefined
  }
}

/**
 * Ordered list — first match wins. Order matters only for ambiguous short prefixes
 * (none at the moment), but keeping it stable makes diffs easier to review.
 */
const SIGNATURES: SignatureEntry[] = [
  // ─── Images ─────────────────────────────────────────────────────────
  { mime: 'image/jpeg', signatures: [[0xff, 0xd8, 0xff]] },
  { mime: 'image/png',  signatures: [[0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]] },
  // GIF87a and GIF89a
  { mime: 'image/gif',  signatures: [
      [0x47, 0x49, 0x46, 0x38, 0x37, 0x61],
      [0x47, 0x49, 0x46, 0x38, 0x39, 0x61],
    ],
  },
  // WebP: "RIFF????WEBP"
  {
    mime: 'image/webp',
    signatures: [[0x52, 0x49, 0x46, 0x46]],
    refine: (buf) => (buf.length >= 12 && buf.subarray(8, 12).toString('ascii') === 'WEBP'
      ? 'image/webp' : undefined),
  },
  { mime: 'image/bmp',  signatures: [[0x42, 0x4d]] },
  // TIFF little-endian + big-endian
  { mime: 'image/tiff', signatures: [[0x49, 0x49, 0x2a, 0x00], [0x4d, 0x4d, 0x00, 0x2a]] },
  { mime: 'image/x-icon', signatures: [[0x00, 0x00, 0x01, 0x00]] },

  // ─── Documents ──────────────────────────────────────────────────────
  { mime: 'application/pdf', signatures: [[0x25, 0x50, 0x44, 0x46, 0x2d]] },
  // Office legacy OLE2 (.doc, .xls, .ppt)
  { mime: 'application/x-cfb',
    signatures: [[0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1]] },

  // ─── Archives ───────────────────────────────────────────────────────
  // ZIP (also covers .docx / .xlsx / .zip / .jar / .apk — detected as zip here;
  // callers restricting a specific office format should include the "application/zip"
  // family in `allowedMimeTypes` too).
  { mime: 'application/zip', signatures: [
      [0x50, 0x4b, 0x03, 0x04],
      [0x50, 0x4b, 0x05, 0x06],
      [0x50, 0x4b, 0x07, 0x08],
    ],
  },
  { mime: 'application/x-rar-compressed',
    signatures: [[0x52, 0x61, 0x72, 0x21, 0x1a, 0x07, 0x00],
                 [0x52, 0x61, 0x72, 0x21, 0x1a, 0x07, 0x01, 0x00]] },
  { mime: 'application/x-7z-compressed',
    signatures: [[0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c]] },
  { mime: 'application/gzip', signatures: [[0x1f, 0x8b]] },

  // ─── Audio / Video ──────────────────────────────────────────────────
  // ISO BMFF: ftyp box at bytes 4..8
  {
    mime: 'video/mp4',
    signatures: [{ offset: 4, bytes: [0x66, 0x74, 0x79, 0x70] }],
    refine: refineIsoBmff,
  },
  // MP3 ID3v2 header
  { mime: 'audio/mpeg', signatures: [[0x49, 0x44, 0x33]] },
  // WAV: "RIFF????WAVE"
  {
    mime: 'audio/wav',
    signatures: [[0x52, 0x49, 0x46, 0x46]],
    refine: (buf) => (buf.length >= 12 && buf.subarray(8, 12).toString('ascii') === 'WAVE'
      ? 'audio/wav' : undefined),
  },
  // OGG container
  { mime: 'audio/ogg', signatures: [[0x4f, 0x67, 0x67, 0x53]] },
  // FLAC
  { mime: 'audio/flac', signatures: [[0x66, 0x4c, 0x61, 0x43]] },
  // WebM (Matroska)
  { mime: 'video/webm', signatures: [[0x1a, 0x45, 0xdf, 0xa3]] },

  // ─── Executables (security-relevant: callers restricting images must reject these) ─
  // PE / EXE
  { mime: 'application/vnd.microsoft.portable-executable',
    signatures: [[0x4d, 0x5a]] },
  // ELF
  { mime: 'application/x-elf', signatures: [[0x7f, 0x45, 0x4c, 0x46]] },
  // Mach-O (both endian + fat)
  { mime: 'application/x-mach-binary',
    signatures: [[0xfe, 0xed, 0xfa, 0xce], [0xfe, 0xed, 0xfa, 0xcf],
                 [0xce, 0xfa, 0xed, 0xfe], [0xcf, 0xfa, 0xed, 0xfe],
                 [0xca, 0xfe, 0xba, 0xbe]] },
]

function matchesSignature(buf: Buffer, sig: number[] | { offset: number; bytes: number[] }): boolean {
  if (Array.isArray(sig)) {
    if (buf.length < sig.length) return false
    for (let i = 0; i < sig.length; i++) if (buf[i] !== sig[i]) return false
    return true
  }
  if (buf.length < sig.offset + sig.bytes.length) return false
  for (let i = 0; i < sig.bytes.length; i++) {
    if (buf[sig.offset + i] !== sig.bytes[i]) return false
  }
  return true
}

/**
 * Detect a MIME type from the first bytes of a file.
 * Returns `undefined` when no signature in our list matches.
 */
export function detectMimeFromBuffer(buf: Buffer): string | undefined {
  if (!buf || buf.length < 2) return undefined
  for (const entry of SIGNATURES) {
    for (const sig of entry.signatures) {
      if (!matchesSignature(buf, sig)) continue
      if (entry.refine) {
        // Refine is authoritative for container formats (RIFF/WEBP vs RIFF/WAVE,
        // ISO BMFF ftyp brands, …). If it cannot confirm the sub-type, we do NOT
        // fall back to the entry's default MIME — otherwise RIFF/XXXX would be
        // misreported as WEBP, defeating the signature check entirely.
        const refined = entry.refine(buf)
        if (refined) return refined
        continue
      }
      return entry.mime
    }
  }
  return undefined
}

/**
 * Returns true when the detected and declared MIME belong to the same high-level
 * family. We keep the comparison loose on purpose:
 *   * `image/jpeg`  === `image/jpg`       (common client typo)
 *   * `application/zip` is accepted for `application/vnd.openxmlformats-...`
 *     (Office OOXML files share the ZIP signature; callers who want to reject
 *     non-office zips should list the specific subtype anyway)
 * The uploader uses this to decide whether a magic-byte mismatch is worth
 * refusing the upload for.
 */
export function mimeFamiliesMatch(detected: string, declared: string): boolean {
  const d = normalizeMime(detected)
  const c = normalizeMime(declared)
  if (d === c) return true

  // Treat 'image/jpg' as an alias of 'image/jpeg'.
  const JPEG = new Set(['image/jpeg', 'image/jpg', 'image/pjpeg'])
  if (JPEG.has(d) && JPEG.has(c)) return true

  // OOXML office documents are physically ZIP; accept zip signature for them.
  const OOXML_PREFIXES = [
    'application/vnd.openxmlformats-officedocument',
    'application/vnd.ms-excel',
    'application/vnd.ms-powerpoint',
    'application/msword',
    'application/vnd.ms-',
  ]
  if (d === 'application/zip' && OOXML_PREFIXES.some((p) => c.startsWith(p))) return true

  // Legacy OLE2 formats (.doc, .xls, .ppt) share the x-cfb signature.
  if (d === 'application/x-cfb') {
    if (
      c === 'application/msword' ||
      c === 'application/vnd.ms-excel' ||
      c === 'application/vnd.ms-powerpoint' ||
      c.startsWith('application/vnd.ms-')
    ) {
      return true
    }
  }

  return false
}

function normalizeMime(s: string): string {
  return (s || '').trim().toLowerCase().split(';')[0].trim()
}

/**
 * Wrap an upload stream so the first chunk is held long enough to detect the
 * actual MIME, then released unchanged. The caller gets:
 *   - `stream`: a drop-in replacement for the original body stream
 *   - `detected`: a promise that resolves to the detected MIME (or undefined)
 *
 * We buffer up to SIGNATURE_PEEK_BYTES, run the detector once, then flush and
 * stop buffering. If the source emits less than that before 'end', we still
 * detect on whatever we have. No chunks are swallowed.
 */
export function wrapWithSignatureDetection(source: Readable): {
  stream: PassThrough
  detected: Promise<string | undefined>
} {
  const out = new PassThrough()
  let peek: Buffer[] = []
  let peekLen = 0
  let flushed = false
  let detectedResolve!: (v: string | undefined) => void
  const detected = new Promise<string | undefined>((r) => (detectedResolve = r))

  const flush = (final: boolean): string | undefined => {
    if (flushed) return undefined
    flushed = true
    const buf = peek.length === 1 ? peek[0] : Buffer.concat(peek, peekLen)
    const mime = detectMimeFromBuffer(buf)
    detectedResolve(mime)
    // We did NOT consume the bytes — they are already written to `out` below.
    peek = []
    return mime
  }

  source.on('data', (chunk: Buffer) => {
    if (!flushed) {
      peek.push(chunk)
      peekLen += chunk.length
      if (peekLen >= SIGNATURE_PEEK_BYTES) flush(false)
    }
    // Always pass through — detector never drops data.
    if (!out.write(chunk)) source.pause()
  })
  out.on('drain', () => source.resume())
  source.on('end', () => {
    if (!flushed) flush(true)
    out.end()
  })
  source.on('error', (err) => {
    if (!flushed) {
      flushed = true
      detectedResolve(undefined)
    }
    out.destroy(err)
  })

  return { stream: out, detected }
}
