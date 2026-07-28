import { describe, expect, it } from 'vitest'
import { normalizeNullableSchema } from './openapi-nullable'

describe('normalizeNullableSchema', () => {
  it('rewrites a two-element type array with null into type + nullable', () => {
    const schema = {
      type: 'object',
      properties: {
        file_size_limit: { type: ['integer', 'null'] },
        allowed_mime_types: { type: ['array', 'null'], items: { type: 'string' } },
      },
    }

    expect(normalizeNullableSchema(schema)).toEqual({
      type: 'object',
      properties: {
        file_size_limit: { type: 'integer', nullable: true },
        allowed_mime_types: { type: 'array', nullable: true, items: { type: 'string' } },
      },
    })
  })

  it('rewrites a two-branch anyOf with a null-only branch into the other branch + nullable', () => {
    const schema = {
      type: 'object',
      properties: {
        id: { anyOf: [{ type: 'string' }, { type: 'null' }] },
        metadata: {
          anyOf: [{ $ref: 'someSchema#' }, { type: 'null' }],
        },
      },
    }

    expect(normalizeNullableSchema(schema)).toEqual({
      type: 'object',
      properties: {
        id: { type: 'string', nullable: true },
        metadata: { $ref: 'someSchema#', nullable: true },
      },
    })
  })

  it('handles the null-only branch appearing first', () => {
    const schema = { anyOf: [{ type: 'null' }, { type: 'string' }] }

    expect(normalizeNullableSchema(schema)).toEqual({ type: 'string', nullable: true })
  })

  it('leaves a genuine multi-type union untouched', () => {
    const schema = {
      anyOf: [
        { type: 'integer', minimum: 0 },
        { type: 'string', pattern: '^[0-9]+$' },
      ],
    }

    expect(normalizeNullableSchema(schema)).toEqual(schema)
  })

  it('leaves anyOf with more than two branches untouched', () => {
    const schema = {
      anyOf: [{ required: ['a'] }, { required: ['b'] }, { required: ['c'] }],
    }

    expect(normalizeNullableSchema(schema)).toEqual(schema)
  })

  it('recurses through arrays and nested items', () => {
    const schema = {
      type: 'array',
      items: {
        anyOf: [{ type: 'integer' }, { type: 'null' }],
      },
    }

    expect(normalizeNullableSchema(schema)).toEqual({
      type: 'array',
      items: { type: 'integer', nullable: true },
    })
  })

  it('does not mutate the source schema', () => {
    const schema = {
      type: 'object',
      properties: {
        value: { type: ['integer', 'null'] },
      },
    }

    const normalized = normalizeNullableSchema(schema)

    expect(normalized).not.toBe(schema)
    expect(schema.properties.value).toEqual({ type: ['integer', 'null'] })
  })
})
