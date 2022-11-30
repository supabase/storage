export const transformationOptionsSchema = {
  height: { type: 'integer', examples: [100], minimum: 0 },
  width: { type: 'integer', examples: [100], minimum: 0 },
  resize: { type: 'string', enum: ['cover', 'contain', 'fill'] },
} as const
