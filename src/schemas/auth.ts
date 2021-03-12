export const authSchema = {
  $id: 'authSchema',
  type: 'object',
  properties: {
    authorization: { type: 'string' },
  },
  required: ['authorization'],
} as const
