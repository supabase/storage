import { Knex } from 'knex'

/**
 * Creates a mock for Knex query builder that always resolves to the provided value
 *
 * @param returnedValue
 */
export function createMockKnexReturning(returnedValue: object | object[]): Knex {
  // Create a fake "thenable" query builder
  const queryBuilder = {
    insert: vi.fn().mockReturnThis(),
    select: vi.fn().mockReturnThis(),
    where: vi.fn().mockReturnThis(),
    update: vi.fn().mockReturnThis(),
    del: vi.fn().mockReturnThis(),
    returning: vi.fn().mockReturnThis(),
    first: vi.fn().mockResolvedValue(returnedValue),
    then: (resolve: (val: object | object[]) => void) =>
      Promise.resolve(returnedValue).then(resolve),
    onConflict: {},
  }

  // These need access to the full queryBuilder to return the right `this`
  queryBuilder.onConflict = vi.fn(() => ({
    ignore: vi.fn(() => queryBuilder),
    merge: vi.fn(() => queryBuilder),
  }))

  // Mock knex function and .table()
  const mockKnex = Object.assign(vi.fn().mockReturnValue(queryBuilder), {
    table: vi.fn().mockReturnValue(queryBuilder),
  }) as unknown as Knex

  return mockKnex
}
