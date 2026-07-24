import type { CustomTypesConfig } from 'pg'
import { describe, expect, it, vi } from 'vitest'
import { createPostgresTypeParsers } from './type-parsers'

describe('PostgreSQL type parsers', () => {
  it('parses textual int8 values as numbers', () => {
    const fallback = vi.fn()
    const typeParsers = createPostgresTypeParsers({
      getTypeParser: fallback,
    } as CustomTypesConfig)

    const parseInt8 = typeParsers.getTypeParser(20, 'text')

    expect(parseInt8('42')).toBe(42)
    expect(parseInt8('-42')).toBe(-42)
    expect(fallback).not.toHaveBeenCalled()
  })

  it('delegates binary int8 and unrelated types to the base parsers', () => {
    const parseFallback = vi.fn((value: unknown) => value)
    const getTypeParser = vi.fn(() => parseFallback)
    const typeParsers = createPostgresTypeParsers({
      getTypeParser,
    } as CustomTypesConfig)

    expect(typeParsers.getTypeParser(20, 'binary')).toBe(parseFallback)
    expect(typeParsers.getTypeParser(25, 'text')).toBe(parseFallback)
    expect(getTypeParser).toHaveBeenNthCalledWith(1, 20, 'binary')
    expect(getTypeParser).toHaveBeenNthCalledWith(2, 25, 'text')
  })
})
