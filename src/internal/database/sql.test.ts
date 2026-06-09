import { quoteIdentifier, quoteQualifiedIdentifier } from './sql'

describe('quoteIdentifier', () => {
  it('quotes valid PostgreSQL identifiers', () => {
    expect(quoteIdentifier('storage')).toBe('"storage"')
    expect(quoteIdentifier('_tenant_1')).toBe('"_tenant_1"')
  })

  it('rejects identifiers that require escaping or qualification', () => {
    expect(() => quoteIdentifier('tenant-id')).toThrow('Invalid PostgreSQL identifier')
    expect(() => quoteIdentifier('storage.objects')).toThrow('Invalid PostgreSQL identifier')
    expect(() => quoteIdentifier('"id"')).toThrow('Invalid PostgreSQL identifier')
  })
})

describe('quoteQualifiedIdentifier', () => {
  it('quotes schema-qualified PostgreSQL identifiers', () => {
    expect(quoteQualifiedIdentifier('storage.objects')).toBe('"storage"."objects"')
  })

  it('rejects unqualified or over-qualified table names', () => {
    expect(() => quoteQualifiedIdentifier('objects')).toThrow('Invalid PostgreSQL table name')
    expect(() => quoteQualifiedIdentifier('storage.objects.extra')).toThrow(
      'Invalid PostgreSQL table name'
    )
  })
})
