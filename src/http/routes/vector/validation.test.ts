import { compileNoCoercionValidator } from './validation'

describe('compileNoCoercionValidator', () => {
  it('validates referenced schemas without coercing scalar types', () => {
    const primitiveSchema = {
      $id: 'testVectorPrimitive',
      oneOf: [{ type: 'string' }, { type: 'boolean' }, { type: 'number' }],
    } as const
    const bodySchema = {
      type: 'object',
      properties: {
        flag: { type: 'boolean' },
        metadataValue: { $ref: 'testVectorPrimitive#' },
        score: { type: 'number' },
      },
      required: ['flag', 'metadataValue', 'score'],
      additionalProperties: false,
    } as const

    const validate = compileNoCoercionValidator(bodySchema, [primitiveSchema])({
      schema: bodySchema,
    } as never)
    const validBody = {
      flag: true,
      metadataValue: 0.75,
      score: 0.75,
    }
    const invalidBody = {
      flag: 'true',
      metadataValue: '0.75',
      score: '0.75',
    }

    expect(validate(validBody)).toEqual({ value: validBody })
    expect(validate(invalidBody)).toEqual({
      error: expect.arrayContaining([
        expect.objectContaining({
          instancePath: '/flag',
          keyword: 'type',
        }),
        expect.objectContaining({
          instancePath: '/score',
          keyword: 'type',
        }),
      ]),
    })
  })
})
