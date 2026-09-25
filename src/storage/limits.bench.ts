/**
 * Micro-benchmarks for isValidKey.
 *
 * Purpose: measure the runtime cost of the new UTF-8 + invisible-glyph
 * validator against the legacy ASCII-only whitelist regex. Every Supabase
 * Storage request calls isValidKey at least once (via mustBeValidKey), so
 * a slower validator has a direct multiplier on request throughput.
 *
 * Run: `npx vitest bench --config vitest.unit.config.ts src/storage/limits.bench.ts`
 */

import { bench, describe } from 'vitest'
import { isValidKey } from './limits'

// The exact whitelist the module used before the UTF-8 change. Kept here so
// the benchmark measures the direct A/B cost, not the cost against some
// unrelated legacy shape.
const LEGACY_VALID_OBJECT_KEY = /^[A-Za-z0-9_/!.*'() &$=@;:+,?-]*$/
function legacyIsValidKey(key: string): boolean {
  return key.length > 0 && LEGACY_VALID_OBJECT_KEY.test(key)
}

// Representative workload — the kind of key names a Supabase Storage instance
// actually gets. Mix of Latin, non-Latin, deep paths, and short keys.
const CORPUS = [
  'avatar.png',
  'user-uploads/2026-09-25/report.pdf',
  'buckets/photos/vacation/DSC_0142.jpg',
  'temp/backup-2026-09-25T00-00-00Z.tar.gz',
  'org/team_42/project-alpha/design/final.fig',
  'a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t.bin',
  '文档/2026年报告.pdf',
  'archivos/informe-anual.docx',
  'документы/отчет.txt',
  'ドキュメント/仕様書.md',
  'documents/résumé.pdf',
  'assets/emoji-🚀-branding.png',
  'user_' + 'a'.repeat(200) + '.dat',
  'nested/'.repeat(30) + 'leaf.txt',
]

// Second corpus: keys the validator must REJECT — measures rejection path.
const BAD_CORPUS = [
  'file\x00.txt', // null byte
  'file\t.txt', // tab
  'file#.txt', // S3-avoid
  'file<.txt', // S3-avoid
  'file\\.txt', // backslash — actually caught after this PR
  'test​.txt', // zero-width space attack
  'test‮exe.txt', // RTL override spoof
  '﻿test.txt', // BOM prefix
  '../etc/passwd', // — wait, path traversal is up to caller, not us
  '', // empty
]

describe('isValidKey — accept path', () => {
  bench('legacy ASCII-only validator', () => {
    for (const k of CORPUS) legacyIsValidKey(k)
  })

  bench('new UTF-8 + invisible-glyph validator', () => {
    for (const k of CORPUS) isValidKey(k)
  })
})

describe('isValidKey — reject path', () => {
  bench('legacy ASCII-only validator (rejects UTF-8 as a side effect)', () => {
    for (const k of BAD_CORPUS) legacyIsValidKey(k)
  })

  bench('new UTF-8 + invisible-glyph validator', () => {
    for (const k of BAD_CORPUS) isValidKey(k)
  })
})

describe('isValidKey — single-call throughput', () => {
  const short = 'avatar.png'
  const long = 'nested/'.repeat(50) + 'file-' + '文书'.repeat(20) + '.pdf'

  bench('legacy · short Latin key', () => {
    legacyIsValidKey(short)
  })
  bench('new · short Latin key', () => {
    isValidKey(short)
  })

  bench('legacy · long mixed-script key (rejected)', () => {
    legacyIsValidKey(long)
  })
  bench('new · long mixed-script key (accepted)', () => {
    isValidKey(long)
  })
})
