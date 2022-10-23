export const authSchema = {
  $id: 'authSchema',
  type: 'object',
  properties: {
    authorization: {
      type: 'string',
      examples: [
        'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24ifQ.625_WdcF3KHqz5amU0x2X5WWHP-OEs_4qj0ssLNHzTs',
      ],
    },
  },
  required: ['authorization'],
} as const
