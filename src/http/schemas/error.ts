export const errorSchema = {
  $id: 'errorSchema',
  type: 'object',
  properties: {
    statusCode: { type: 'string' },
    error: { type: 'string' },
    message: { type: 'string' },
  },
  required: ['statusCode', 'error', 'message'],
} as const
