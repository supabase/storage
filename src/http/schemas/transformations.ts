export const transformationQueryString = {
  height: { type: 'integer', examples: [100], minimum: 0 },
  width: { type: 'integer', examples: [100], minimum: 0 },
  resize: { type: 'string', enum: ['fill', 'fit', 'fill-down', 'force', 'auto'] },
} as const
