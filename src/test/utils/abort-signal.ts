import { vi } from 'vitest'

export function spyOnAbortSignalTimeout() {
  const timeoutSignal = new AbortController().signal
  const timeoutSpy = vi.spyOn(AbortSignal, 'timeout').mockReturnValue(timeoutSignal)

  return { timeoutSignal, timeoutSpy }
}

export function spyOnAbortSignalAny() {
  const anySignal = new AbortController().signal
  const anySpy = vi.spyOn(AbortSignal, 'any').mockReturnValue(anySignal)

  return { anySignal, anySpy }
}
