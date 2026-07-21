export const transformationOptionsSchema = {
  height: { type: 'integer', finite: true, examples: [100], minimum: 0 },
  width: { type: 'integer', finite: true, examples: [100], minimum: 0 },
  resize: { type: 'string', enum: ['cover', 'contain', 'fill'] },
  format: { type: 'string', enum: ['origin', 'avif', 'webp'] },
  quality: { type: 'integer', finite: true, minimum: 20, maximum: 100 },
} as const
