import { vi } from 'vitest'

export type SentWattMessage = {
  application: string
  data: Record<string, unknown>
  message: string
}

export function installWattMessagingMock(
  responses: Record<string, unknown> = {}
): { sent: SentWattMessage[] } {
  const sent: SentWattMessage[] = []

  ;(globalThis as typeof globalThis & { platformatic?: unknown }).platformatic = {
    messaging: {
      send: vi.fn(async (application: string, message: string, data: Record<string, unknown>) => {
        sent.push({ application, data, message })
        const response = responses[message]
        return response instanceof Promise ? response : response
      }),
    },
  }

  return { sent }
}
