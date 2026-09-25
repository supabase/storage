/**
 * Fuzz test for isValidKey.
 *
 * Purpose: prove empirically that the validator behaves as specified across
 * a large adversarial input space. We generate ~10,000 crafted inputs from
 * distinct attack families and check that:
 *   (1) every input is answered in bounded time (no ReDoS)
 *   (2) accept/reject verdicts match a byte-level oracle that inspects each
 *       code point independently (no compound regex surprise)
 *   (3) mean-time-per-call stays under a strict budget
 *
 * These tests do not replace unit tests — they catch classes of bugs that
 * hand-written cases miss.
 */

import { describe, expect, it } from 'vitest'
import { isValidKey } from './limits'

// -----------------------------------------------------------------------------
// Oracle: a per-code-point implementation that mirrors the regex intent.
// If the regex and the oracle disagree, one of them is wrong — we log every
// disagreement in the failure message so a maintainer can see the diff.
// -----------------------------------------------------------------------------
const FORBIDDEN_ASCII = new Set<number>([
  0x22, 0x23, 0x25, 0x3c, 0x3e, 0x5b, 0x5c, 0x5d, 0x5e, 0x60, 0x7b, 0x7c, 0x7d, 0x7e,
])

function oracleIsValidKey(key: string): boolean {
  if (key.length === 0) return false
  for (const ch of key) {
    const cp = ch.codePointAt(0)!
    // ASCII controls + DEL
    if (cp <= 0x1f || cp === 0x7f) return false
    // S3 "characters to avoid"
    if (FORBIDDEN_ASCII.has(cp)) return false
    // Invisible-glyph attacks
    if (
      cp === 0x200b ||
      cp === 0x200e ||
      cp === 0x200f ||
      cp === 0x2028 ||
      cp === 0x2029 ||
      (cp >= 0x202a && cp <= 0x202e) ||
      (cp >= 0x2066 && cp <= 0x2069) ||
      cp === 0xfeff
    ) {
      return false
    }
  }
  return true
}

// -----------------------------------------------------------------------------
// Corpora — attack families
// -----------------------------------------------------------------------------

const SCRIPTS: Array<[string, string]> = [
  ['Latin extended', 'àéîõùçÑÜß'],
  ['Cyrillic', 'документ ключ'],
  ['Greek', 'αρχείο κλειδί'],
  ['Arabic', 'ملف مفتاح'],
  ['Hebrew', 'קובץ מפתח'],
  ['CJK', '文档 檔案 ドキュメント 문서'],
  ['Devanagari', 'दस्तावेज़ फ़ाइल'],
  ['Thai', 'ไฟล์เอกสาร'],
  ['Tamil', 'கோப்பு ஆவணம்'],
  ['Emoji BMP', '☎️♻️⚡'],
  ['Emoji astral', '😀🚀🎨🇺🇸'],
  ['ZWJ family', '👨‍👩‍👧‍👦'],
]

const ATTACK_CODEPOINTS: number[] = [
  0x00,
  0x01,
  0x08,
  0x0a,
  0x0d,
  0x1f,
  0x7f, // controls
  0x22,
  0x23,
  0x25,
  0x3c,
  0x3e,
  0x5b,
  0x5c,
  0x5d, // S3-avoid
  0x5e,
  0x60,
  0x7b,
  0x7c,
  0x7d,
  0x7e, // S3-avoid
  0x200b,
  0x200e,
  0x200f, // invisible
  0x2028,
  0x2029, // line/para sep
  0x202a,
  0x202b,
  0x202c,
  0x202d,
  0x202e, // BiDi
  0x2066,
  0x2067,
  0x2068,
  0x2069, // isolate
  0xfeff, // BOM
]

// A tiny deterministic PRNG so failures are reproducible.
function mulberry32(seed: number) {
  let state = seed
  return function () {
    state = (state + 0x6d2b79f5) | 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

function randomString(rng: () => number, len: number, includeAttack: boolean): string {
  let s = ''
  const attackAt = includeAttack ? Math.floor(rng() * len) : -1
  for (let i = 0; i < len; i++) {
    if (i === attackAt) {
      s += String.fromCodePoint(ATTACK_CODEPOINTS[Math.floor(rng() * ATTACK_CODEPOINTS.length)])
      continue
    }
    // Random code point across the printable BMP + occasional astral
    if (rng() < 0.05) {
      // Astral
      s += String.fromCodePoint(0x1f300 + Math.floor(rng() * 0x1000))
    } else {
      // Printable BMP > U+00A0 excluding the attack ranges (best-effort)
      let cp: number
      do {
        cp = 0x20 + Math.floor(rng() * (0xd800 - 0x20))
      } while (
        cp <= 0x20 ||
        (cp >= 0x200b && cp <= 0x200f) ||
        (cp >= 0x2028 && cp <= 0x202e) ||
        (cp >= 0x2066 && cp <= 0x2069) ||
        cp === 0xfeff
      )
      s += String.fromCodePoint(cp)
    }
  }
  return s
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

describe('isValidKey — fuzz (10,000 inputs)', () => {
  it('matches the per-code-point oracle on all fuzz inputs', () => {
    const rng = mulberry32(0xdeadbeef)
    const disagreements: Array<{ input: string; regex: boolean; oracle: boolean }> = []

    // 1. Every real-world script (positive)
    for (const [_name, sample] of SCRIPTS) {
      const regex = isValidKey(sample)
      const oracle = oracleIsValidKey(sample)
      if (regex !== oracle) disagreements.push({ input: sample, regex, oracle })
    }

    // 2. Each attack code point in isolation and inside a normal string
    for (const cp of ATTACK_CODEPOINTS) {
      const bare = String.fromCodePoint(cp)
      const wrapped = `abc${bare}xyz`
      for (const s of [bare, wrapped]) {
        const regex = isValidKey(s)
        const oracle = oracleIsValidKey(s)
        if (regex !== oracle) disagreements.push({ input: s, regex, oracle })
      }
    }

    // 3. ~10,000 random strings of varying lengths
    for (let i = 0; i < 10_000; i++) {
      const len = 1 + Math.floor(rng() * 200)
      const includeAttack = rng() < 0.5
      const s = randomString(rng, len, includeAttack)
      const regex = isValidKey(s)
      const oracle = oracleIsValidKey(s)
      if (regex !== oracle) {
        disagreements.push({ input: s, regex, oracle })
        if (disagreements.length > 20) break // fail fast, log first 20
      }
    }

    if (disagreements.length > 0) {
      const dump = disagreements
        .slice(0, 10)
        .map(
          (d, i) =>
            `  #${i + 1} regex=${d.regex} oracle=${d.oracle} ` +
            `codepoints=[${[...d.input].map((c) => 'U+' + c.codePointAt(0)!.toString(16)).join(',')}]`
        )
        .join('\n')
      throw new Error(`${disagreements.length} regex/oracle disagreements:\n${dump}`)
    }
  })

  it('never exceeds a 5ms per-call budget on adversarial input (no ReDoS)', () => {
    // Classic ReDoS-shaped inputs: long repeated safe prefix + trailing junk
    const adversarial: string[] = [
      'a'.repeat(10_000),
      'a'.repeat(5_000) + '\x00',
      'a'.repeat(5_000) + '\u{200B}',
      '/'.repeat(5_000),
      String.fromCodePoint(0x1f680).repeat(2_000), // astral
      String.fromCodePoint(0x1f468, 0x200d, 0x1f469).repeat(300), // ZWJ family
      'x\u{202E}'.repeat(1_000), // repeated BiDi
    ]

    for (const s of adversarial) {
      const start = process.hrtime.bigint()
      isValidKey(s)
      const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6
      expect(elapsedMs).toBeLessThan(5)
    }
  })

  it('mean call cost on realistic keys stays under 5µs', () => {
    const rng = mulberry32(42)
    const keys: string[] = []
    for (let i = 0; i < 1_000; i++) {
      keys.push(randomString(rng, 20 + Math.floor(rng() * 60), false))
    }

    const start = process.hrtime.bigint()
    for (const k of keys) isValidKey(k)
    const elapsedNs = Number(process.hrtime.bigint() - start)
    const meanNs = elapsedNs / keys.length

    // 5µs / call is very generous — real number should be well under 1µs.
    expect(meanNs).toBeLessThan(5_000)
  })
})
