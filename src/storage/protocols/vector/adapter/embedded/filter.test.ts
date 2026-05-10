import { translateFilter } from './filter'

describe('translateFilter', () => {
  describe('implicit equality', () => {
    it('translates string equality', () => {
      expect(translateFilter({ category: 'foo' })).toBe("category = 'foo'")
    })
    it('translates number equality', () => {
      expect(translateFilter({ n: 5 })).toBe('n = 5')
    })
    it('translates boolean equality', () => {
      expect(translateFilter({ active: true })).toBe('active = true')
      expect(translateFilter({ active: false })).toBe('active = false')
    })
    it('escapes single quotes in strings', () => {
      expect(translateFilter({ title: "it's" })).toBe("title = 'it''s'")
    })
    it('handles negative numbers', () => {
      expect(translateFilter({ n: -3.5 })).toBe('n = -3.5')
    })
  })

  describe('field operators', () => {
    it('$eq', () => {
      expect(translateFilter({ category: { $eq: 'foo' } })).toBe("category = 'foo'")
    })
    it('$ne', () => {
      expect(translateFilter({ category: { $ne: 'foo' } })).toBe("category != 'foo'")
    })
    it('$gt', () => {
      expect(translateFilter({ n: { $gt: 5 } })).toBe('n > 5')
    })
    it('$gte', () => {
      expect(translateFilter({ n: { $gte: 5 } })).toBe('n >= 5')
    })
    it('$lt', () => {
      expect(translateFilter({ n: { $lt: 5 } })).toBe('n < 5')
    })
    it('$lte', () => {
      expect(translateFilter({ n: { $lte: 5 } })).toBe('n <= 5')
    })
    it('$in with strings', () => {
      expect(translateFilter({ tag: { $in: ['a', 'b', 'c'] } })).toBe("tag IN ('a', 'b', 'c')")
    })
    it('$in with numbers', () => {
      expect(translateFilter({ n: { $in: [1, 2, 3] } })).toBe('n IN (1, 2, 3)')
    })
    it('$nin', () => {
      expect(translateFilter({ tag: { $nin: ['a', 'b'] } })).toBe("tag NOT IN ('a', 'b')")
    })
    it('$exists true', () => {
      expect(translateFilter({ tag: { $exists: true } })).toBe('tag IS NOT NULL')
    })
    it('$exists false', () => {
      expect(translateFilter({ tag: { $exists: false } })).toBe('tag IS NULL')
    })
  })

  describe('multi-operator on a single field (implicit AND)', () => {
    it('combines $gt and $lt with AND', () => {
      expect(translateFilter({ n: { $gt: 1, $lt: 10 } })).toBe('(n > 1 AND n < 10)')
    })
    it('combines $gte and $lte', () => {
      expect(translateFilter({ n: { $gte: 0, $lte: 100 } })).toBe('(n >= 0 AND n <= 100)')
    })
  })

  describe('multi-field clause (implicit AND)', () => {
    it('joins field clauses with AND', () => {
      expect(translateFilter({ a: 1, b: 'x' })).toBe("a = 1 AND b = 'x'")
    })
  })

  describe('logical operators', () => {
    it('$and joins clauses with AND', () => {
      expect(
        translateFilter({
          $and: [{ a: 1 }, { b: 'x' }],
        })
      ).toBe("(a = 1) AND (b = 'x')")
    })
    it('$or joins clauses with OR', () => {
      expect(
        translateFilter({
          $or: [{ a: 1 }, { b: 'x' }],
        })
      ).toBe("(a = 1) OR (b = 'x')")
    })
    it('nested $and inside $or', () => {
      expect(
        translateFilter({
          $or: [{ $and: [{ a: 1 }, { b: 2 }] }, { c: 3 }],
        })
      ).toBe('((a = 1) AND (b = 2)) OR (c = 3)')
    })
    it('deeply nested mix', () => {
      expect(
        translateFilter({
          $and: [{ $or: [{ a: 1 }, { b: 2 }] }, { c: { $gte: 5 } }],
        })
      ).toBe('((a = 1) OR (b = 2)) AND (c >= 5)')
    })
  })

  describe('invalid inputs', () => {
    it('rejects non-object filter', () => {
      expect(() => translateFilter('foo' as never)).toThrow()
      expect(() => translateFilter([] as never)).toThrow()
      expect(() => translateFilter(null as never)).toThrow()
    })
    it('rejects empty filter', () => {
      expect(() => translateFilter({})).toThrow()
    })
    it('rejects empty $and', () => {
      expect(() => translateFilter({ $and: [] })).toThrow()
    })
    it('rejects empty $or', () => {
      expect(() => translateFilter({ $or: [] })).toThrow()
    })
    it('rejects $and mixed with field key', () => {
      expect(() => translateFilter({ $and: [{ a: 1 }], b: 2 } as never)).toThrow()
    })
    it('rejects unknown $-prefixed key at field position', () => {
      expect(() => translateFilter({ $weird: 1 } as never)).toThrow()
    })
    it('rejects unknown field operator', () => {
      expect(() => translateFilter({ a: { $weird: 1 } } as never)).toThrow()
    })
    it('rejects empty operator object', () => {
      expect(() => translateFilter({ a: {} as never })).toThrow()
    })
    it('rejects $in with empty array', () => {
      expect(() => translateFilter({ a: { $in: [] } })).toThrow()
    })
    it('rejects $nin with empty array', () => {
      expect(() => translateFilter({ a: { $nin: [] } })).toThrow()
    })
    it('rejects $exists with non-boolean', () => {
      expect(() => translateFilter({ a: { $exists: 'yes' as never } })).toThrow()
    })
    it('rejects invalid identifier', () => {
      expect(() => translateFilter({ 'evil; drop': 1 })).toThrow()
    })
    it('rejects NaN', () => {
      expect(() => translateFilter({ a: NaN })).toThrow()
    })
    it('rejects Infinity', () => {
      expect(() => translateFilter({ a: Infinity })).toThrow()
    })
  })
})
