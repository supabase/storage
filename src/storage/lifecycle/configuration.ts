import { createHash } from 'node:crypto'
import {
  type BucketLifecycleConfiguration,
  LIFECYCLE_MAX_NEWER_NONCURRENT_VERSIONS,
  LIFECYCLE_MAX_RULES,
  type LifecycleRule,
} from '../schemas/lifecycle'

type ValidationCategory = 'MALFORMED_XML' | 'INVALID_ARGUMENT' | 'INVALID_REQUEST'

const INVALID_NONCURRENT_DAYS_MESSAGE =
  "'NoncurrentDays' for NoncurrentVersionExpiration action must be a positive integer"
const INVALID_NEWER_NONCURRENT_VERSIONS_MESSAGE =
  "'NewerNoncurrentVersions' for NoncurrentVersionExpiration action must be an integer between 1 and 100"
const LEGACY_NEWER_NONCURRENT_VERSIONS_MESSAGE =
  'NewerNoncurrentVersions element can only be used in Lifecycle V2.'
const S3_FILTER_ELEMENTS = new Set([
  'And',
  'ObjectSizeGreaterThan',
  'ObjectSizeLessThan',
  'Prefix',
  'Tag',
])
const S3_UNSUPPORTED_RULE_ELEMENTS = new Set([
  'AbortIncompleteMultipartUpload',
  'Expiration',
  'NoncurrentVersionTransition',
  'Transition',
])

interface RuleShape {
  id: string
  status: string
  filter: string
  prefix: string
  expiration: string
  s3: boolean
}

const CANONICAL_RULE_SHAPE: RuleShape = {
  id: 'id',
  status: 'status',
  filter: 'filter',
  prefix: 'legacyPrefix',
  expiration: 'noncurrentVersionExpiration',
  s3: false,
}

const S3_RULE_SHAPE: RuleShape = {
  id: 'ID',
  status: 'Status',
  filter: 'Filter',
  prefix: 'Prefix',
  expiration: 'NoncurrentVersionExpiration',
  s3: true,
}

export class LifecycleConfigurationValidationError extends Error {
  constructor(
    message: string,
    public readonly category: ValidationCategory = 'MALFORMED_XML'
  ) {
    super(message)
    this.name = 'LifecycleConfigurationValidationError'
  }
}

export function normalizeLifecycleConfiguration(input: unknown): BucketLifecycleConfiguration {
  const root = requireRecord(input, 'Lifecycle configuration must be an object')
  if (Object.hasOwn(root, 'LifecycleConfiguration')) {
    if (Object.hasOwn(root, 'rules')) {
      throw validationError(
        'Lifecycle configuration must not mix rules with LifecycleConfiguration'
      )
    }
    assertOnlyKeys(root, ['LifecycleConfiguration'], 'Lifecycle configuration')
  }
  const configuration = Object.hasOwn(root, 'LifecycleConfiguration')
    ? requireRecord(root.LifecycleConfiguration, 'LifecycleConfiguration must be an object')
    : root

  if (Object.hasOwn(configuration, 'rules')) {
    assertOnlyKeys(configuration, ['rules'], 'Lifecycle configuration')
    const rules = requireArray(
      configuration.rules,
      'Lifecycle configuration rules must be an array'
    )
    return normalizeCanonicalRules(rules)
  }

  assertOnlyKeys(configuration, ['Rule', '$'], 'LifecycleConfiguration')
  assertS3NamespaceAttributes(configuration.$, 'LifecycleConfiguration')
  const rawRules = Array.isArray(configuration.Rule)
    ? configuration.Rule
    : configuration.Rule === undefined
      ? []
      : [configuration.Rule]

  return normalizeS3Rules(rawRules)
}

export function lifecycleConfigurationToS3(
  configuration: BucketLifecycleConfiguration
): Record<string, unknown> {
  return {
    LifecycleConfiguration: {
      Rule: configuration.rules.map((rule) => ({
        ...(rule.id === undefined ? {} : { ID: rule.id }),
        Status: rule.status,
        ...(rule.legacyPrefix === undefined ? { Filter: '' } : { Prefix: rule.legacyPrefix }),
        ...(rule.noncurrentVersionExpiration === undefined
          ? {}
          : {
              NoncurrentVersionExpiration: {
                NoncurrentDays: rule.noncurrentVersionExpiration.noncurrentDays,
                ...(rule.noncurrentVersionExpiration.newerNoncurrentVersions === undefined
                  ? {}
                  : {
                      NewerNoncurrentVersions:
                        rule.noncurrentVersionExpiration.newerNoncurrentVersions,
                    }),
              },
            }),
      })),
    },
  }
}

export function lifecycleConfigurationsEqual(
  left: BucketLifecycleConfiguration | null,
  right: BucketLifecycleConfiguration
): boolean {
  if (left === null || !Array.isArray(left.rules) || left.rules.length !== right.rules.length) {
    return false
  }

  const rightRulesById = new Map(right.rules.map((rule) => [rule.id, rule]))
  if (rightRulesById.size !== right.rules.length) return false

  return left.rules.every((rule) => {
    if (rule === null || typeof rule !== 'object' || Array.isArray(rule)) return false

    const candidate = rightRulesById.get(rule.id)
    if (candidate === undefined || !lifecycleRulesEqual(rule, candidate)) return false

    rightRulesById.delete(rule.id)
    return true
  })
}

function lifecycleRulesEqual(left: LifecycleRule, right: LifecycleRule): boolean {
  return (
    left.id === right.id &&
    left.status === right.status &&
    left.legacyPrefix === right.legacyPrefix &&
    lifecycleFiltersEqual(left.filter, right.filter) &&
    left.noncurrentVersionExpiration?.noncurrentDays ===
      right.noncurrentVersionExpiration?.noncurrentDays &&
    left.noncurrentVersionExpiration?.newerNoncurrentVersions ===
      right.noncurrentVersionExpiration?.newerNoncurrentVersions
  )
}

function lifecycleFiltersEqual(
  left: LifecycleRule['filter'],
  right: LifecycleRule['filter']
): boolean {
  if (left === undefined || right === undefined) return left === right
  if (typeof left !== 'object' || left === null || Array.isArray(left)) return false
  const leftKeys = Object.keys(left) as (keyof typeof left)[]
  const rightKeys = Object.keys(right)
  return leftKeys.length === rightKeys.length && leftKeys.every((key) => left[key] === right[key])
}

function normalizeCanonicalRules(rawRules: unknown[]): BucketLifecycleConfiguration {
  validateRuleCount(rawRules)
  const rules = rawRules.map((value, index) => normalizeCanonicalRule(value, index))
  const identifiedRules = assignGeneratedRuleIds(rules)
  validateUniqueRuleIds(identifiedRules)
  return { rules: identifiedRules }
}

function normalizeCanonicalRule(value: unknown, index: number): LifecycleRule {
  const rule = requireRecord(value, `Rule ${index + 1} must be an object`)
  assertOnlyKeys(
    rule,
    ['id', 'status', 'filter', 'legacyPrefix', 'noncurrentVersionExpiration'],
    `Rule ${index + 1}`
  )
  return normalizeRule(rule, index, CANONICAL_RULE_SHAPE)
}

function normalizeS3Rules(rawRules: unknown[]): BucketLifecycleConfiguration {
  validateRuleCount(rawRules)
  const rules = rawRules.map((value, index) => normalizeS3Rule(value, index))

  const identifiedRules = assignGeneratedRuleIds(rules)
  validateUniqueRuleIds(identifiedRules)
  return { rules: identifiedRules }
}

function normalizeS3Rule(value: unknown, index: number): LifecycleRule {
  const rule = requireRecord(value, `Rule ${index + 1} must be an object`)
  const supportedKeys = ['ID', 'Status', 'Filter', 'Prefix', 'NoncurrentVersionExpiration']
  const unknownKeys = Object.keys(rule).filter((key) => !supportedKeys.includes(key))

  if (unknownKeys.length > 0) {
    const category = unknownKeys.every((key) => S3_UNSUPPORTED_RULE_ELEMENTS.has(key))
      ? 'INVALID_REQUEST'
      : 'MALFORMED_XML'
    throw validationError(
      `Rule ${index + 1} contains unsupported element ${unknownKeys[0]}`,
      category
    )
  }

  if (!Object.hasOwn(rule, S3_RULE_SHAPE.expiration)) {
    throw validationError(
      `Rule ${index + 1} must contain NoncurrentVersionExpiration`,
      'INVALID_REQUEST'
    )
  }

  return normalizeRule(rule, index, S3_RULE_SHAPE)
}

function normalizeRule(
  rule: Record<string, unknown>,
  index: number,
  shape: RuleShape
): LifecycleRule {
  const isLegacy = usesLegacyPrefix(rule, shape.filter, shape.prefix, index)
  const id = optionalRuleId(rule[shape.id], index)
  const status = normalizeStatus(rule[shape.status], index)
  const expirationInput = rule[shape.expiration]
  if (isLegacy) assertLegacyRuleHasNoCount(expirationInput, index, shape.s3)
  const expiration = normalizeExpiration(expirationInput, index, shape.s3)
  const selector = isLegacy
    ? { legacyPrefix: normalizeLegacyPrefix(rule[shape.prefix], index, shape.s3) }
    : { filter: normalizeFilter(rule[shape.filter], index, shape.s3) }

  return {
    ...(id === undefined ? {} : { id }),
    status,
    ...selector,
    noncurrentVersionExpiration: expiration,
  }
}

function normalizeStatus(value: unknown, index: number): LifecycleRule['status'] {
  if (value !== 'Enabled' && value !== 'Disabled') {
    throw validationError(`Rule ${index + 1} Status must be Enabled or Disabled`)
  }
  return value
}

function normalizeFilter(value: unknown, index: number, s3Shape = false): Record<string, never> {
  if (value === '') return {}
  const filter = requireRecord(value, `Rule ${index + 1} Filter must be an object`)
  const keys = Object.keys(filter)
  if (keys.length === 0) return {}
  if (s3Shape && keys.length === 1 && filter.Prefix === '') return {}

  throw validationError(
    `Rule ${index + 1} uses a lifecycle filter that is not supported in v1`,
    s3Shape && keys.length === 1 && S3_FILTER_ELEMENTS.has(keys[0])
      ? 'INVALID_REQUEST'
      : 'MALFORMED_XML'
  )
}

function normalizeLegacyPrefix(value: unknown, index: number, s3Shape = false): '' {
  if (value === '') return ''
  if (typeof value !== 'string') {
    throw validationError(`Rule ${index + 1} Prefix must be a string`)
  }
  throw validationError(
    `Rule ${index + 1} uses a lifecycle filter that is not supported in v1`,
    s3Shape ? 'INVALID_REQUEST' : 'MALFORMED_XML'
  )
}

function usesLegacyPrefix(
  rule: Record<string, unknown>,
  filterKey: string,
  prefixKey: string,
  index: number
): boolean {
  const hasFilter = Object.hasOwn(rule, filterKey)
  const hasPrefix = Object.hasOwn(rule, prefixKey)
  if (hasFilter === hasPrefix) {
    throw validationError(`Rule ${index + 1} must contain exactly one of Filter or Prefix`)
  }
  return hasPrefix
}

function assertLegacyRuleHasNoCount(value: unknown, index: number, s3Shape: boolean): void {
  const expiration = requireRecord(
    value,
    `Rule ${index + 1} NoncurrentVersionExpiration must be an object`
  )
  const newerKey = s3Shape ? 'NewerNoncurrentVersions' : 'newerNoncurrentVersions'
  if (Object.hasOwn(expiration, newerKey)) {
    throw validationError(LEGACY_NEWER_NONCURRENT_VERSIONS_MESSAGE, 'INVALID_REQUEST')
  }
}

function normalizeExpiration(value: unknown, index: number, s3Shape: boolean) {
  const expiration = requireRecord(
    value,
    `Rule ${index + 1} NoncurrentVersionExpiration must be an object`
  )
  const daysKey = s3Shape ? 'NoncurrentDays' : 'noncurrentDays'
  const newerKey = s3Shape ? 'NewerNoncurrentVersions' : 'newerNoncurrentVersions'
  assertOnlyKeys(expiration, [daysKey, newerKey], `Rule ${index + 1} expiration`)

  if (!Object.hasOwn(expiration, daysKey)) {
    throw validationError(
      `Rule ${index + 1} NoncurrentVersionExpiration must contain NoncurrentDays`
    )
  }

  const noncurrentDays = parseIntegerArgument(expiration[daysKey], INVALID_NONCURRENT_DAYS_MESSAGE)
  if (noncurrentDays < 1) {
    throw validationError(INVALID_NONCURRENT_DAYS_MESSAGE, 'INVALID_ARGUMENT')
  }

  const rawNewer = expiration[newerKey]
  if (rawNewer === undefined) return { noncurrentDays }

  const newerNoncurrentVersions = parseIntegerArgument(
    rawNewer,
    INVALID_NEWER_NONCURRENT_VERSIONS_MESSAGE
  )
  if (
    newerNoncurrentVersions < 1 ||
    newerNoncurrentVersions > LIFECYCLE_MAX_NEWER_NONCURRENT_VERSIONS
  ) {
    throw validationError(INVALID_NEWER_NONCURRENT_VERSIONS_MESSAGE, 'INVALID_ARGUMENT')
  }
  return { noncurrentDays, newerNoncurrentVersions }
}

function optionalRuleId(value: unknown, index: number): string | undefined {
  if (value === undefined || value === '') return undefined
  if (typeof value !== 'string') {
    throw validationError(`Rule ${index + 1} ID must be a string no longer than 255 characters`)
  }
  // AWS S3 counts UTF-16 code units, which matches JavaScript string.length.
  if (value.length > 255) {
    throw validationError(
      `Rule ${index + 1} ID must be 255 characters or fewer`,
      'INVALID_ARGUMENT'
    )
  }
  return value
}

function assignGeneratedRuleIds(rules: LifecycleRule[]): LifecycleRule[] {
  const usedIds = new Set(rules.flatMap((rule) => (rule.id === undefined ? [] : [rule.id])))
  return rules.map((rule) => {
    if (rule.id !== undefined) return { ...rule }

    const content = lifecycleRuleContent(rule)
    const base = `rule-${createHash('sha256').update(content).digest('hex')}`
    let id = base
    let collision = 0
    while (usedIds.has(id)) {
      collision += 1
      id = `${base}-${collision}`
    }
    usedIds.add(id)
    return { ...rule, id }
  })
}

function lifecycleRuleContent(rule: LifecycleRule): string {
  const { id: _id, ...content } = rule
  return stableJsonStringify(content)
}

function stableJsonStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(stableJsonStringify).join(',')}]`
  }
  if (value !== null && typeof value === 'object') {
    const record = value as Record<string, unknown>
    return `{${Object.keys(record)
      .filter((key) => record[key] !== undefined)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableJsonStringify(record[key])}`)
      .join(',')}}`
  }
  return JSON.stringify(value) ?? 'null'
}

function validateRuleCount(rules: unknown[]) {
  if (rules.length < 1 || rules.length > LIFECYCLE_MAX_RULES) {
    throw validationError(
      `Lifecycle configuration must contain between 1 and ${LIFECYCLE_MAX_RULES} rules`
    )
  }
}

function validateUniqueRuleIds(rules: LifecycleRule[]) {
  const ids = new Set<string>()
  for (const rule of rules) {
    if (rule.id === undefined) continue
    if (ids.has(rule.id)) {
      throw validationError(
        'Rule ID must be unique. Found same ID for more than one rule',
        'INVALID_ARGUMENT'
      )
    }
    ids.add(rule.id)
  }
}

function parseIntegerArgument(value: unknown, message: string): number {
  const digits = typeof value === 'string' ? value.trim() : value
  const parsed =
    typeof digits === 'number'
      ? digits
      : typeof digits === 'string' && /^\d+$/.test(digits)
        ? Number(digits)
        : Number.NaN
  if (!Number.isSafeInteger(parsed)) {
    throw validationError(message, 'INVALID_ARGUMENT')
  }
  return parsed
}

function requireRecord(value: unknown, message: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw validationError(message)
  }
  return value as Record<string, unknown>
}

function requireArray(value: unknown, message: string): unknown[] {
  if (!Array.isArray(value)) throw validationError(message)
  return value
}

function assertOnlyKeys(value: Record<string, unknown>, allowed: string[], label: string) {
  const unknown = Object.keys(value).find((key) => !allowed.includes(key))
  if (unknown !== undefined) {
    throw validationError(`${label} contains unsupported field ${unknown}`)
  }
}

function assertS3NamespaceAttributes(value: unknown, label: string) {
  if (value === undefined) return
  const attributes = requireRecord(value, `${label} attributes must be an object`)
  assertOnlyKeys(attributes, ['xmlns'], `${label} attributes`)
  if (attributes.xmlns !== 'http://s3.amazonaws.com/doc/2006-03-01/') {
    throw validationError(`${label} has an invalid XML namespace`)
  }
}

function validationError(message: string, category: ValidationCategory = 'MALFORMED_XML') {
  return new LifecycleConfigurationValidationError(message, category)
}
