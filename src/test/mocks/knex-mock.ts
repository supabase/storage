import { Knex } from 'knex'

/**
 * Creates a mock for Knex query builder that always resolves to the provided value
 *
 * @param returnedValue
 */
export function createMockKnexReturning(returnedValue: object | object[]): Knex {
  // Create a fake "thenable" query builder
  const queryBuilder = {
    insert: jest.fn().mockReturnThis(),
    select: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    update: jest.fn().mockReturnThis(),
    del: jest.fn().mockReturnThis(),
    returning: jest.fn().mockReturnThis(),
    first: jest.fn().mockResolvedValue(returnedValue),
    then: (resolve: (val: object | object[]) => void) =>
      Promise.resolve(returnedValue).then(resolve),
    onConflict: {},
  }

  // These need access to the full queryBuilder to return the right `this`
  queryBuilder.onConflict = jest.fn(() => ({
    ignore: jest.fn(() => queryBuilder),
    merge: jest.fn(() => queryBuilder),
  }))

  // Mock knex function and .table()
  const mockKnex = Object.assign(jest.fn().mockReturnValue(queryBuilder), {
    table: jest.fn().mockReturnValue(queryBuilder),
  }) as unknown as Knex

  return mockKnex
}
