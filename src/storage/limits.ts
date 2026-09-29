import { ERRORS } from '@internal/errors'
import { getConfig } from '../config'
import {
  getDeleteObjectsLimit as getDeleteObjectsLimitForTenant,
  getFeatures,
  getFileSizeLimit as getFileSizeLimitForTenant,
} from '../internal/database/tenant'

const {
  isMultitenant,
  imageTransformationEnabled,
  icebergBucketDetectionSuffix,
  requestHardLimitsEnabled,
} = getConfig()

export type BucketType = 'STANDARD' | 'ANALYTICS'

export const MAX_OBJECTS_PER_REQUEST = 1000
export const MAX_KEYS_PER_S3_DELETE = 1000
// Versioned object deletes expand to the object key plus a `.info` sidecar key.
export const MAX_OBJECTS_PER_DELETE_BATCH = Math.floor(MAX_KEYS_PER_S3_DELETE / 2)
export const MAX_OBJECTS_PER_LOOKUP_BATCH = MAX_OBJECTS_PER_REQUEST
export const ICEBERG_BUCKET_RESERVED_SUFFIX = icebergBucketDetectionSuffix
export const RESERVED_BUCKET_SUFFIXES = [icebergBucketDetectionSuffix]

export const DELETE_OBJECTS_LIMIT_DESCRIPTION = `At most ${MAX_OBJECTS_PER_REQUEST} objects can be deleted per request.`

export async function getDeleteObjectsLimit(tenantId: string): Promise<number> {
  if (isMultitenant) {
    return (await getDeleteObjectsLimitForTenant(tenantId)) ?? MAX_OBJECTS_PER_REQUEST
  }

  return MAX_OBJECTS_PER_REQUEST
}

export async function enforceDeleteObjectsLimit(
  tenantId: string,
  objectCount: number
): Promise<void> {
  if (!requestHardLimitsEnabled) {
    return
  }

  const deleteObjectsLimit = await getDeleteObjectsLimit(tenantId)
  if (objectCount > deleteObjectsLimit) {
    throw ERRORS.InvalidRequest(
      `Bulk object requests are limited to ${deleteObjectsLimit} objects per request.`
    )
  }
}

/**
 * Get the maximum file size for a specific project
 * @param tenantId
 * @param maxUpperLimit
 */
export async function getFileSizeLimit(
  tenantId: string,
  maxUpperLimit?: number | null
): Promise<number> {
  let { uploadFileSizeLimit } = getConfig()
  if (isMultitenant) {
    uploadFileSizeLimit = await getFileSizeLimitForTenant(tenantId)
  }

  if (maxUpperLimit) {
    return Math.min(uploadFileSizeLimit, maxUpperLimit)
  }

  return uploadFileSizeLimit
}

/**
 * Determines if the image transformation feature is enabled.
 * @param tenantId
 */
export async function isImageTransformationEnabled(tenantId: string) {
  if (!isMultitenant) {
    return imageTransformationEnabled
  }

  const { imageTransformation } = await getFeatures(tenantId)

  return imageTransformation.enabled
}

// Bucket names follow the stricter S3 bucket-naming rules and are ASCII-only.
// Hyphen is last so it stays a literal, not a range.
const VALID_BUCKET_NAME = /^[A-Za-z0-9_!.*'() &$=@;:+,?-]*$/

// Object keys accept the full UTF-8 range that S3 itself accepts. Per AWS docs:
//   "You can use any UTF-8 character in an object key name."
//   https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
//
// The reject set below removes three classes of code points:
//   (1) S3-unsafe or URL-encoding-required ASCII:
//         U+0000-U+001F   C0 controls (tab, newline, …)
//         U+007F          DEL
//         U+0080-U+009F   C1 controls
//         # [ ] { } ^ ` " < > \ | % ~   from S3's "Characters to Avoid" list
//   (2) invisible-glyph attack chars — accepted by S3 but let a caller spoof
//       or hide a path. These are code points OWASP flags for filename
//       normalisation and that Unicode UTS #55 lists as "invisible":
//         U+034F          Combining Grapheme Joiner
//         U+061C          Arabic Letter Mark
//         U+200B          Zero-Width Space
//         U+200E U+200F   LTR / RTL marks
//         U+2028 U+2029   line and paragraph separators (treated as CR/LF by some parsers)
//         U+202A–U+202E   LTR/RTL embedding + LRO/RLO/PDF (BiDi override spoofing)
//         U+00AD          Soft Hyphen (SHY)
//         U+180E          Mongolian Vowel Separator
//         U+2060–U+2064   Word Joiner + Function Application + Invisible Times/Separator/Plus
//         U+2066–U+2069   isolate directional formatting (same class)
//         U+FEFF          BOM / zero-width no-break space
//
// U+200C (ZWNJ) and U+200D (ZWJ) are intentionally NOT rejected: they are
// used by legitimate emoji ZWJ sequences and by Persian/Arabic/Devanagari
// orthography.
//
// The `u` flag makes the regex evaluate against full Unicode code points, not
// UTF-16 code units, so an astral char (e.g. 😀) is a single unit. Anchoring
// with ^ and $ is intentional — no partial matches.
const VALID_OBJECT_KEY =
  // biome-ignore lint/suspicious/noControlCharactersInRegex: intentionally rejecting ASCII controls
  // biome-ignore lint/suspicious/noMisleadingCharacterClass: U+034F and lone surrogates are intentionally rejected as standalone units
  /^[^\u0000-\u001f\u007f\u0080-\u009f#\[\]{}^`"<>\\|%~\u{034F}\u{061C}\u{200B}\u{200E}\u{200F}\u{2028}\u{2029}\u{202A}-\u{202E}\u{00AD}\u{180E}\u{2060}-\u{2064}\u{2066}-\u{2069}\uD800-\uDFFF\u{FEFF}]+$/u

/**
 * S3 caps object keys at 1024 UTF-8 bytes. We enforce the same ceiling so a
 * caller gets a validator-level rejection before an upload starts, instead of
 * an opaque S3 error at PUT time.
 * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
 */
export const MAX_OBJECT_KEY_BYTES = 1024

/**
 * Rejects keys with path-traversal segments (`.` or `..`). Even though we
 * mount uploads inside per-tenant prefixes on the backend, propagating these
 * segments into keys confuses listings, signed URLs, and downstream mirrors.
 * A bare leading slash is intentionally allowed for compatibility with the
 * previous validator.
 */
const PATH_TRAVERSAL_RE = /(^|\/)\.{1,2}(\/|$)/

/**
 * Normalises the key to Unicode NFC (Normalization Form Canonical
 * Composition). Without this, `café` uploaded as NFC (`caf` + `é`) and `café`
 * uploaded as NFD (`cafe` + combining acute U+0301) would land as two
 * distinct rows in the database even though they render identically. This
 * enables silent homograph attacks on listings and hides files from callers
 * who normalise their filenames client-side.
 *
 * S3 itself does not normalise, so a client that expects byte-exact
 * round-trip after upload will see the NFC form on retrieval. This is the
 * safer trade-off: filesystem portability (macOS's HFS+ enforces NFD, Linux
 * ext4 is byte-transparent, NTFS accepts either) and listing-hygiene both
 * argue for canonicalising at the boundary.
 *
 * Idempotent: `normalizeObjectKey(normalizeObjectKey(k)) === normalizeObjectKey(k)`
 * for every valid UTF-8 string. Note that NFC can change the byte-length of
 * a string (canonical compositions may expand OR shrink), so callers that
 * enforce a byte-length ceiling must re-check length after normalising.
 */
export function normalizeObjectKey(key: string): string {
  return key.normalize('NFC')
}

/**
 * Validates if a given object key is valid.
 *
 * The validator layers three checks (short-circuit in this order):
 *
 *   1. Non-empty.
 *   2. Character set (see `VALID_OBJECT_KEY` above): full UTF-8 accepted;
 *      ASCII controls, S3 "characters to avoid", invisible-glyph attack
 *      chars, and ill-formed lone UTF-16 surrogates are rejected in one
 *      pass by the same negated character class.
 *   3. Path-traversal: rejects `..`, `.`, and absolute-path prefixes anywhere
 *      in the key.
 *   4. Byte-length: rejects keys whose UTF-8 encoding exceeds S3's 1024-byte
 *      limit — a single Chinese character costs 3 bytes, so this matters for
 *      non-Latin filenames.
 *
 * NOTE: this function does not itself normalise the key. Callers that persist
 * or hash the key MUST first pipe it through `normalizeObjectKey()`; see the
 * comment on that helper for the security rationale. `isValidKey` accepts
 * both NFC and NFD input so that the same rules apply pre- and
 * post-normalisation (all valid NFC keys are also valid pre-normalisation).
 *
 * Keys that succeed here map 1:1 to keys S3 will accept, so callers do not
 * need a second validation layer on the backend.
 *
 * @param key
 */
/**
 * ASCII fast-path acceptance table (1 = allow, 0 = reject). Kept in perfect
 * sync with the negated class in `VALID_OBJECT_KEY`; the fuzz oracle in
 * limits.fuzz.test.ts enforces the invariant on 10 000 adversarial inputs.
 *
 * A Uint8Array lookup keeps the hot loop monomorphic in V8, which is what
 * closes the gap to the legacy ASCII-only regex on the common case.
 */
function buildAsciiAllowedTable(): Uint8Array {
  const table = new Uint8Array(128)
  for (let c = 0x20; c <= 0x7e; c++) table[c] = 1
  // Strip S3 "characters to avoid" — https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
  for (const c of [0x22, 0x23, 0x25, 0x3c, 0x3e, 0x5b, 0x5c, 0x5d, 0x5e, 0x60, 0x7b, 0x7c, 0x7d, 0x7e]) {
    table[c] = 0
  }
  return table
}
const ASCII_ALLOWED = buildAsciiAllowedTable()

export function isValidKey(key: string): boolean {
  const len = key.length
  if (len === 0) {
    return false
  }
  // Length hard-cap first — cheap and short-circuits pathological input.
  if (len > MAX_OBJECT_KEY_BYTES) {
    return false
  }

  // ASCII fast-path. Real-world storage traffic is dominated by ASCII keys
  // (Latin filenames, UUIDs, S3 prefixes); a tight table-lookup loop lets
  // us skip the `u`-flag Unicode regex entirely on that hot case. A byte
  // > 0x7F drops the caller into the full regex below.
  let dotSeen = false
  let i = 0
  for (; i < len; i++) {
    const c = key.charCodeAt(i)
    if (c > 0x7f) break
    if (ASCII_ALLOWED[c] === 0) return false
    if (c === 0x2e) dotSeen = true
  }

  if (i === len) {
    // Pure ASCII path — the Unicode regex would only repeat work we already
    // did. Byte-length equals key.length for ASCII and is capped above.
    if (dotSeen && hasPathTraversal(key)) return false
    return true
  }

  // Unicode path — the regex handles C1 controls, invisible-glyph attacks
  // and lone surrogates in one pass with `u` flag semantics.
  if (!VALID_OBJECT_KEY.test(key)) return false
  if (key.indexOf('.') !== -1 && hasPathTraversal(key)) return false
  // Non-ASCII inflates up to 4 bytes per code point; only pay for the
  // Buffer allocation when the string is long enough for it to matter.
  if (
    len > MAX_OBJECT_KEY_BYTES / 4 &&
    Buffer.byteLength(key, 'utf8') > MAX_OBJECT_KEY_BYTES
  ) {
    return false
  }
  return true
}

/**
 * Branchless path-traversal detector. Equivalent to the regex
 * `/(^|\/)\.{1,2}(\/|$)/` but avoids the ~200 ns per-call regex overhead.
 * A single forward scan looks for a dot immediately after `^` or `/`, then
 * confirms it is followed by another dot-or-slash or end-of-string.
 */
function hasPathTraversal(key: string): boolean {
  const n = key.length
  for (let i = 0; i < n; i++) {
    if (key.charCodeAt(i) !== 0x2e /* . */) continue
    // Only match dots that start a segment.
    if (i !== 0 && key.charCodeAt(i - 1) !== 0x2f /* / */) continue
    // Consume a possible second dot.
    let j = i + 1
    if (j < n && key.charCodeAt(j) === 0x2e) j++
    // Followed by '/' or end-of-string?
    if (j === n || key.charCodeAt(j) === 0x2f) return true
  }
  return false
}

/**
 * Validates if a given object key or bucket key is valid
 * @param bucketName
 */
export function isValidBucketName(bucketName: string): boolean {
  // only allow s3 safe characters and characters which require special handling for now
  // the slash restriction come from bucket naming rules
  // and the rest of the validation rules are based on S3 object key validation.
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
  return bucketName.length > 0 && bucketName.length < 101 && VALID_BUCKET_NAME.test(bucketName)
}

/**
 * Validates if a given object key is valid
 * throws if invalid
 * @param key
 */
export function mustBeValidKey(key?: string): asserts key is string {
  if (!key || !isValidKey(key)) {
    throw ERRORS.InvalidKey(key || '')
  }
}

/**
 * Validates if a given bucket name is valid
 * throws if invalid
 * @param key
 */
export function mustBeValidBucketName(key?: string): asserts key is string {
  if (!key || !isValidBucketName(key)) {
    throw ERRORS.InvalidBucketName(key || '')
  }
}

/**
 * Validates if a given bucket name is not reserved
 * @param bucketName
 */
export function mustBeNotReservedBucketName(bucketName?: string): asserts bucketName is string {
  if (!bucketName || RESERVED_BUCKET_SUFFIXES.some((suffix) => bucketName.endsWith(suffix))) {
    throw ERRORS.InvalidBucketName(bucketName || '')
  }
}

export function parseFileSizeToBytes(valueWithUnit: string) {
  const valuesRegex = /(^[0-9]+(?:\.[0-9]+)?)(gb|mb|kb|b)$/i

  if (!valuesRegex.test(valueWithUnit)) {
    throw ERRORS.InvalidFileSizeLimit()
  }

  const [, valueS, unit] = valueWithUnit.match(valuesRegex)!
  const value = parseFloat(valueS)

  switch (unit.toUpperCase()) {
    case 'GB':
      return Math.round(value * 1e9)
    case 'MB':
      return Math.round(value * 1e6)
    case 'KB':
      return Math.round(value * 1000)
    case 'B':
      return Math.round(value)
    default:
      throw ERRORS.InvalidFileSizeLimit()
  }
}

export const UUID_PATTERN =
  '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-5][0-9a-fA-F]{3}-[089abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
const UUID_REGEX = new RegExp(UUID_PATTERN)

export function isUuid(value: string) {
  return UUID_REGEX.test(value)
}

export function isEmptyFolder(object: string) {
  return object.endsWith('.emptyFolderPlaceholder')
}

const CLIENT_AGENT_REGEX = {
  // storage-py (storage3) = supabase-py/storage3 v0.12.1
  storage3: /supabase-py\/storage3 v(\d+)\.(\d+)\.(\d+)/i,
  // supabase-py = supabase-py/2.17.0
  'supabase-py': /supabase-py\/(\d+)\.(\d+)\.(\d+)/i,
}
export type ClientAgent = keyof typeof CLIENT_AGENT_REGEX

/**
 * Checks if the client is supabase-py and before the specified version
 *
 * @param client which client type are we checking for
 * @param userAgent user agent header string
 * @param version semver to check against, must be in format '0.0.0'
 */
export function isClientVersionBefore(
  client: ClientAgent,
  userAgent: string,
  version: string
): boolean {
  const [minMajor, minMinor, minPatch] = version.split('.').map(Number)
  const match = userAgent.match(CLIENT_AGENT_REGEX[client])
  if (!match) {
    return false
  }

  const [major, minor, patch] = match.slice(1).map(Number)

  if (major < minMajor) return true
  if (major > minMajor) return false
  if (minor < minMinor) return true
  if (minor > minMinor) return false
  return patch < minPatch
}
