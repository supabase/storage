import { getAcceptanceConfig } from './config'
import { getBackendTotalBytes, postUsageLedgerBaseline } from './usage-ledger'

// No-ops against a remote/unmanaged target, where ACCEPTANCE_USAGE_LEDGER_URL is never set.
export default async function setup() {
  const { usageLedgerUrl: ledgerUrl, usagePrefix } = getAcceptanceConfig()
  if (!ledgerUrl) {
    return
  }

  const baselineBytes = await getBackendTotalBytes({ prefix: usagePrefix })
  await postUsageLedgerBaseline(ledgerUrl, baselineBytes)
}
