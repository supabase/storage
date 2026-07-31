export const fileSizeLimitSchema = {
  anyOf: [
    { type: ['integer', 'null'], finite: true, examples: [1000], minimum: 0 },
    {
      type: ['string', 'null'],
      pattern: '^[0-9]+(?:\\.[0-9]+)?(?:[gG][bB]|[mM][bB]|[kK][bB]|[bB])$',
      examples: ['100MB'],
    },
  ],
} as const
