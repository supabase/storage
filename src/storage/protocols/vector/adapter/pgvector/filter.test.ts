import { translateFilter, translateFilterToPositionalPlaceholders } from './filter'

function eqSql(column: string, field: number) {
  const node = `${column}->$${field}`
  const value = `$${field + 1}::jsonb`
  return `(${node} = ${value} OR (jsonb_typeof(${node}) = 'array' AND ${node} @> jsonb_build_array(${value})))`
}

function neSql(column: string, field: number) {
  const node = `${column}->$${field}`
  const value = `$${field + 1}::jsonb`
  return `(CASE WHEN jsonb_typeof(${node}) = 'array' THEN NOT (${node} @> jsonb_build_array(${value})) ELSE ${node} <> ${value} END)`
}

function inSql(column: string, field: number) {
  const node = `${column}->$${field}`
  const values = `$${field + 1}::jsonb[]`
  return `(${node} = ANY(${values}) OR EXISTS (SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(${node}) = 'array' THEN ${node} ELSE '[]'::jsonb END) AS elem(value) WHERE elem.value = ANY(${values})))`
}

function ninSql(column: string, field: number) {
  const node = `${column}->$${field}`
  const values = `$${field + 1}::jsonb[]`
  return `(CASE WHEN jsonb_typeof(${node}) = 'array' THEN NOT EXISTS (SELECT 1 FROM jsonb_array_elements(${node}) AS elem(value) WHERE elem.value = ANY(${values})) ELSE ${node} <> ALL(${values}) END)`
}

describe('translateFilter (pgvector / JSONB)', () => {
  describe('implicit equality', () => {
    it('string', () => {
      expect(translateFilter({ category: 'foo' })).toEqual({
        sql: eqSql('metadata', 1),
        params: ['category', '"foo"'],
      })
    })
    it('number', () => {
      expect(translateFilter({ n: 5 })).toEqual({
        sql: eqSql('metadata', 1),
        params: ['n', '5'],
      })
    })
    it('boolean', () => {
      expect(translateFilter({ active: true })).toEqual({
        sql: eqSql('metadata', 1),
        params: ['active', 'true'],
      })
    })
    it('preserves embedded quotes via parameter (no manual escaping needed)', () => {
      expect(translateFilter({ title: "it's" })).toEqual({
        sql: eqSql('metadata', 1),
        params: ['title', `"it's"`],
      })
    })
    it('matches primitive filters against list-valued metadata', () => {
      expect(translateFilter({ category: 'documentary' })).toEqual({
        sql: eqSql('metadata', 1),
        params: ['category', '"documentary"'],
      })
    })
  })

  describe('accepts arbitrary metadata key strings', () => {
    it('keys with hyphens', () => {
      expect(translateFilter({ 'user-id': 'abc' })).toEqual({
        sql: eqSql('metadata', 1),
        params: ['user-id', '"abc"'],
      })
    })
    it('keys with dots', () => {
      expect(translateFilter({ 'my.key': 'v' })).toEqual({
        sql: eqSql('metadata', 1),
        params: ['my.key', '"v"'],
      })
    })
    it('keys with spaces', () => {
      expect(translateFilter({ 'a b c': 'v' })).toEqual({
        sql: eqSql('metadata', 1),
        params: ['a b c', '"v"'],
      })
    })
    it('keys that look hostile are still passed as parameters', () => {
      expect(translateFilter({ "'; DROP TABLE x;--": 'v' })).toEqual({
        sql: eqSql('metadata', 1),
        params: ["'; DROP TABLE x;--", '"v"'],
      })
    })
  })

  describe('field operators', () => {
    it('$eq', () => {
      expect(translateFilter({ category: { $eq: 'foo' } })).toEqual({
        sql: eqSql('metadata', 1),
        params: ['category', '"foo"'],
      })
    })
    it('$ne', () => {
      expect(translateFilter({ category: { $ne: 'foo' } })).toEqual({
        sql: neSql('metadata', 1),
        params: ['category', '"foo"'],
      })
    })
    it('$gt guards with jsonb_typeof + numeric cast', () => {
      expect(translateFilter({ n: { $gt: 5 } })).toEqual({
        sql: "(jsonb_typeof(metadata->$1) = 'number' AND (metadata->>$2)::numeric > $3)",
        params: ['n', 'n', 5],
      })
    })
    it('$gte', () => {
      expect(translateFilter({ n: { $gte: 5 } })).toEqual({
        sql: "(jsonb_typeof(metadata->$1) = 'number' AND (metadata->>$2)::numeric >= $3)",
        params: ['n', 'n', 5],
      })
    })
    it('$lt', () => {
      expect(translateFilter({ n: { $lt: 5 } })).toEqual({
        sql: "(jsonb_typeof(metadata->$1) = 'number' AND (metadata->>$2)::numeric < $3)",
        params: ['n', 'n', 5],
      })
    })
    it('$lte', () => {
      expect(translateFilter({ n: { $lte: 5 } })).toEqual({
        sql: "(jsonb_typeof(metadata->$1) = 'number' AND (metadata->>$2)::numeric <= $3)",
        params: ['n', 'n', 5],
      })
    })
    it.for([
      ['$in', inSql],
      ['$nin', ninSql],
    ] as const)('%s preserves primitive types in array bindings', ([op, sql]) => {
      expect(translateFilter({ value: { [op]: [1, '1', true, 'true'] } })).toEqual({
        sql: sql('metadata', 1),
        params: ['value', ['1', '"1"', 'true', '"true"']],
      })
    })
    it('$exists true uses jsonb_exists (function form avoids positional `?` ambiguity)', () => {
      expect(translateFilter({ tag: { $exists: true } })).toEqual({
        sql: 'jsonb_exists(metadata, $1)',
        params: ['tag'],
      })
    })
    it('$exists false negates', () => {
      expect(translateFilter({ tag: { $exists: false } })).toEqual({
        sql: 'NOT jsonb_exists(metadata, $1)',
        params: ['tag'],
      })
    })
  })

  describe('multi-operator on single field', () => {
    it('combines $gt and $lt with AND', () => {
      expect(translateFilter({ n: { $gt: 1, $lt: 10 } })).toEqual({
        sql:
          "((jsonb_typeof(metadata->$1) = 'number' AND (metadata->>$2)::numeric > $3) AND " +
          "(jsonb_typeof(metadata->$4) = 'number' AND (metadata->>$5)::numeric < $6))",
        params: ['n', 'n', 1, 'n', 'n', 10],
      })
    })
  })

  describe('multi-field (implicit AND)', () => {
    it('joins clauses with AND', () => {
      expect(translateFilter({ a: 1, b: 'x' })).toEqual({
        sql: `${eqSql('metadata', 1)} AND ${eqSql('metadata', 3)}`,
        params: ['a', '1', 'b', '"x"'],
      })
    })
  })

  describe('logical operators', () => {
    it('$and', () => {
      expect(translateFilter({ $and: [{ a: 1 }, { b: 'x' }] })).toEqual({
        sql: `(${eqSql('metadata', 1)}) AND (${eqSql('metadata', 3)})`,
        params: ['a', '1', 'b', '"x"'],
      })
    })
    it('$or', () => {
      expect(translateFilter({ $or: [{ a: 1 }, { b: 'x' }] })).toEqual({
        sql: `(${eqSql('metadata', 1)}) OR (${eqSql('metadata', 3)})`,
        params: ['a', '1', 'b', '"x"'],
      })
    })
    it('nested $and within $or', () => {
      expect(translateFilter({ $or: [{ $and: [{ a: 1 }, { b: 2 }] }, { c: 3 }] })).toEqual({
        sql: `((${eqSql('metadata', 1)}) AND (${eqSql('metadata', 3)})) OR (${eqSql('metadata', 5)})`,
        params: ['a', '1', 'b', '2', 'c', '3'],
      })
    })
    it('deeply nested mix', () => {
      expect(
        translateFilter({ $and: [{ $or: [{ a: 1 }, { b: 2 }] }, { c: { $gte: 5 } }] })
      ).toEqual({
        sql:
          `((${eqSql('metadata', 1)}) OR (${eqSql('metadata', 3)})) AND ` +
          "((jsonb_typeof(metadata->$5) = 'number' AND (metadata->>$6)::numeric >= $7))",
        params: ['a', '1', 'b', '2', 'c', 'c', 5],
      })
    })
  })

  describe('column override', () => {
    it('uses provided column reference', () => {
      expect(translateFilter({ a: 1 }, 'v.metadata')).toEqual({
        sql: eqSql('v.metadata', 1),
        params: ['a', '1'],
      })
    })
  })

  describe('positional placeholder conversion', () => {
    it('expands reused numbered placeholders into positional bindings', () => {
      expect(translateFilterToPositionalPlaceholders({ category: 'cats' })).toEqual({
        sql:
          '(metadata->? = ?::jsonb OR ' +
          "(jsonb_typeof(metadata->?) = 'array' AND metadata->? @> jsonb_build_array(?::jsonb)))",
        params: ['category', '"cats"', 'category', 'category', '"cats"'],
      })
    })
    it('reuses the typed array binding for $in scalar and metadata-array comparisons', () => {
      expect(translateFilterToPositionalPlaceholders({ value: { $in: [1, '1'] } })).toEqual({
        sql:
          '(metadata->? = ANY(?::jsonb[]) OR EXISTS (SELECT 1 FROM ' +
          "jsonb_array_elements(CASE WHEN jsonb_typeof(metadata->?) = 'array' THEN metadata->? ELSE '[]'::jsonb END) " +
          'AS elem(value) WHERE elem.value = ANY(?::jsonb[])))',
        params: ['value', ['1', '"1"'], 'value', 'value', ['1', '"1"']],
      })
    })
  })

  describe('invalid inputs', () => {
    it('rejects non-object', () => {
      expect(() => translateFilter('foo' as never)).toThrow()
      expect(() => translateFilter([] as never)).toThrow()
      expect(() => translateFilter(null as never)).toThrow()
    })
    it('rejects empty filter', () => {
      expect(() => translateFilter({})).toThrow()
    })
    it('rejects empty $and / $or', () => {
      expect(() => translateFilter({ $and: [] })).toThrow()
      expect(() => translateFilter({ $or: [] })).toThrow()
    })
    it('rejects logical operator mixed with field key', () => {
      expect(() => translateFilter({ $and: [{ a: 1 }], b: 2 } as never)).toThrow()
    })
    it('rejects unknown $-prefix at field position', () => {
      expect(() => translateFilter({ $weird: 1 } as never)).toThrow()
    })
    it('rejects unknown field operator', () => {
      expect(() => translateFilter({ a: { $weird: 1 } } as never)).toThrow()
    })
    it('rejects empty operator object', () => {
      expect(() => translateFilter({ a: {} as never })).toThrow()
    })
    it('rejects $in / $nin with empty array', () => {
      expect(() => translateFilter({ a: { $in: [] } })).toThrow()
      expect(() => translateFilter({ a: { $nin: [] } })).toThrow()
    })
    it('rejects $exists with non-boolean', () => {
      expect(() => translateFilter({ a: { $exists: 'yes' as never } })).toThrow()
    })
    it('rejects $gt with non-number', () => {
      expect(() => translateFilter({ a: { $gt: 'x' as never } })).toThrow()
    })
    it('rejects NaN / Infinity', () => {
      expect(() => translateFilter({ a: NaN })).toThrow()
      expect(() => translateFilter({ a: Infinity })).toThrow()
    })
    it('rejects column reference with semicolons', () => {
      expect(() => translateFilter({ a: 1 }, 'evil; drop')).toThrow()
    })
    it('rejects column reference with injection in earlier segment', () => {
      // Previously vulnerable: regex only checked the last segment after the
      // dot. Now the full reference must match identifier-segments.
      expect(() => translateFilter({ a: 1 }, 't; DROP TABLE x --.metadata')).toThrow()
    })
  })
})
