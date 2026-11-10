import { getAcceptanceConfig } from './config'
import {
  drainUsageLedgerViolations,
  fetchUsageLedgerSummary,
  getBackendTotalBytes,
  postUsageLedgerBaseline,
} from './usage-ledger'

// No-ops against a remote/unmanaged target, where ACCEPTANCE_USAGE_LEDGER_URL is never set.
export default async function setup() {
  const { usageLedgerUrl: ledgerUrl, usagePrefix } = getAcceptanceConfig()
  if (!ledgerUrl) {
    return
  }

  const baselineBytes = await getBackendTotalBytes({ prefix: usagePrefix })
  await postUsageLedgerBaseline(ledgerUrl, baselineBytes)

  return async function teardown() {
    const violations = await drainUsageLedgerViolations(ledgerUrl)
    if (violations.length > 0) {
      throw new Error(`Usage checksum violations:\n${violations.join('\n')}`)
    }

    const summary = await fetchUsageLedgerSummary(ledgerUrl)
    if (summary.count === 0) {
      throw new Error(
        'Usage ledger never observed a single [Lifecycle] log during this run - check that LOGFLARE_ENABLED/LOGFLARE_API_BASE_URL were wired into the server env'
      )
    }

    const finalBytes = await getBackendTotalBytes({ prefix: usagePrefix })
    const expectedBackendDelta = finalBytes - summary.baselineBytes
    if (expectedBackendDelta !== summary.totalBytesChanged) {
      throw new Error(
        `Final usage byte mismatch: backend delta=${expectedBackendDelta}, logged bytesChanged sum=${summary.totalBytesChanged}`
      )
    }
  }
}
