export const LIFECYCLE_DISPATCH_LIMIT = 100

export function lifecycleErrorMessage(error: unknown): string {
  const value = error instanceof Error ? error.message : String(error)
  return value.slice(0, 1000)
}
