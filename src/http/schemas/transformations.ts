export const transformationOptionsSchema = {
  height: { type: 'integer', examples: [100], minimum: 0 },
  width: { type: 'integer', examples: [100], minimum: 0 },
  resize: { type: 'string', enum: ['cover', 'contain', 'fill'] },
  format: { type: 'string', enum: ['origin', 'avif'] },
  quality: { type: 'integer', minimum: 20, maximum: 100 },
} as const
