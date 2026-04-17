import { vi } from 'vitest'

// fs-xattr is pulled in transitively through the file backend; it has no Windows build.
vi.mock('fs-xattr', () => ({
  set: vi.fn(() => Promise.resolve()),
  get: vi.fn(() => Promise.resolve(undefined)),
}))

import { isValidHeader } from '@storage/protocols/s3/s3-handler'

// Mirror the constants in s3-handler.ts
const MAX_HEADER_NAME_LENGTH = 1024 * 8
const MAX_HEADER_VALUE_LENGTH = 1024 * 8

describe('isValidHeader', () => {
  it('accepts a typical header name and value', () => {
    expect(isValidHeader('content-type', 'application/json')).toBe(true)
  })

  it('accepts all token chars permitted by RFC7230 §3.2.6', () => {
    expect(isValidHeader("!#$%&'*+-.^_`|~09AZaz", 'v')).toBe(true)
  })

  it('rejects header names containing characters outside the token set', () => {
    expect(isValidHeader('bad name', 'v')).toBe(false)
    expect(isValidHeader('bad:name', 'v')).toBe(false)
    expect(isValidHeader('bad(name)', 'v')).toBe(false)
  })

  it('rejects an empty header name', () => {
    expect(isValidHeader('', 'v')).toBe(false)
  })

  it('rejects header names exceeding the max byte length', () => {
    const oversizedName = 'a'.repeat(MAX_HEADER_NAME_LENGTH + 1)
    expect(isValidHeader(oversizedName, 'value')).toBe(false)
  })

  it('rejects oversized names even when all characters are otherwise valid', () => {
    // Long + regex-matching still has to fail: the length check must not be bypassed.
    const oversizedValid = 'a'.repeat(MAX_HEADER_NAME_LENGTH + 100)
    expect(isValidHeader(oversizedValid, 'ok')).toBe(false)
  })

  it('accepts header names exactly at the max byte length', () => {
    const maxName = 'a'.repeat(MAX_HEADER_NAME_LENGTH)
    expect(isValidHeader(maxName, 'value')).toBe(true)
  })

  it('rejects header values containing control characters', () => {
    expect(isValidHeader('x-custom', 'bad\x00value')).toBe(false)
    expect(isValidHeader('x-custom', 'bad\nvalue')).toBe(false)
  })

  it('rejects header values containing CRLF (header injection)', () => {
    expect(isValidHeader('x-custom', 'innocent\r\nX-Injected: 1')).toBe(false)
  })

  it('rejects header values exceeding the max byte length', () => {
    const oversizedValue = 'a'.repeat(MAX_HEADER_VALUE_LENGTH + 1)
    expect(isValidHeader('x-custom', oversizedValue)).toBe(false)
  })

  it('accepts header values exactly at the max byte length', () => {
    const maxValue = 'a'.repeat(MAX_HEADER_VALUE_LENGTH)
    expect(isValidHeader('x-custom', maxValue)).toBe(true)
  })

  it('accepts an array of values when all are valid', () => {
    expect(isValidHeader('x-custom', ['one', 'two', 'three'])).toBe(true)
  })

  it('rejects an array of values when any are invalid', () => {
    expect(isValidHeader('x-custom', ['ok', 'bad\x00value'])).toBe(false)
  })
})
