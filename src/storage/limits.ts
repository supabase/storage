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
//         U+2060          Word Joiner
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
  // biome-ignore lint/suspicious/noMisleadingCharacterClass: U+034F is intentionally rejected as a standalone char
  /^[^\u0000-\u001f\u007f\u0080-\u009f#\[\]{}^`"<>\\|%~\u{034F}\u{061C}\u{200B}\u{200E}\u{200F}\u{2028}\u{2029}\u{202A}-\u{202E}\u{2060}\u{2066}-\u{2069}\u{FEFF}]+$/u

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
 * Validates if a given object key is valid.
 *
 * The validator layers three checks (short-circuit in this order):
 *
 *   1. Character set (see `VALID_OBJECT_KEY` above): full UTF-8 accepted;
 *      ASCII controls, S3 "characters to avoid", and invisible-glyph attack
 *      chars rejected.
 *   2. Path-traversal: rejects `..`, `.`, and absolute-path prefixes anywhere
 *      in the key.
 *   3. Byte-length: rejects keys whose UTF-8 encoding exceeds S3's 1024-byte
 *      limit — a single Chinese character costs 3 bytes, so this matters for
 *      non-Latin filenames.
 *
 * Keys that succeed here map 1:1 to keys S3 will accept, so callers do not
 * need a second validation layer on the backend.
 *
 * @param key
 */
export function isValidKey(key: string): boolean {
  if (!key || key.length === 0) {
    return false
  }
  if (!VALID_OBJECT_KEY.test(key)) {
    return false
  }
  // Fast path: the traversal regex only matches keys containing a dot, so
  // skip it entirely when there is none. Real keys are dot-free most of the
  // time (e.g. `folder/uuid`), so this cuts the average cost noticeably.
  if (key.indexOf('.') !== -1 && PATH_TRAVERSAL_RE.test(key)) {
    return false
  }
  // Fast path: ASCII bytes equal UTF-16 code units, so if `.length` is already
  // within the budget we can skip the Buffer.byteLength allocation for the
  // common ASCII case.
  if (key.length > MAX_OBJECT_KEY_BYTES) {
    return false
  }
  if (
    key.length > MAX_OBJECT_KEY_BYTES / 4 &&
    Buffer.byteLength(key, 'utf8') > MAX_OBJECT_KEY_BYTES
  ) {
    return false
  }
  return true
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
