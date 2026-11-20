// Retry budget for transaction setup (acquire + BEGIN) when a pooled connection
// turns out to be broken or the server rejects it for connection limits. Shared by
// the direct tenant pools and the Database Watt application.
export const TRANSACTION_SETUP_RETRY: Readonly<{
  maxAttempts: number
  totalBudgetMs: number
  minDelayMs: number
  maxDelayMs: number
}> = Object.freeze({
  maxAttempts: 11,
  totalBudgetMs: 3000,
  minDelayMs: 50,
  maxDelayMs: 200,
})

// Full jitter with exponential backoff: 50-100, 100-200, then 200ms.
export function nextTransactionSetupRetryDelay(delayMs: number): {
  sleepMs: number
  nextDelayMs: number
} {
  return {
    sleepMs: Math.min(delayMs * (1 + Math.random()), TRANSACTION_SETUP_RETRY.maxDelayMs),
    nextDelayMs: Math.min(delayMs * 2, TRANSACTION_SETUP_RETRY.maxDelayMs),
  }
}
