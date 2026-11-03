import { getAcceptanceConfig } from '../support/config'
import { fetchUsageLedgerSummary, getBackendTotalBytes } from '../support/usage-ledger'

/**
 * Checksum on the "[Lifecycle]" usage logs (bytesChanged)
 * The real checks are in `checkpointUsageBytes()`, run from the cleanup helpers while objects still exist
 * This file surfaces anything it caught, since specs clean up after and nets back to ~0 by the time a run finishes
 *
 * Must run last (see acceptance/support/sequencer.ts)
 */
const { usageLedgerUrl: ledgerUrl, usagePrefix } = getAcceptanceConfig()

describe.skipIf(!ledgerUrl)('usage byte checksum', () => {
  it('never recorded a bytesChanged vs. actual-bytes mismatch during the run', async () => {
    const summary = await fetchUsageLedgerSummary(ledgerUrl as string)

    expect(summary.violations, JSON.stringify(summary.violations, null, 2)).toEqual([])
    expect(
      summary.count,
      'the ledger never observed a single [Lifecycle] log during this run - check that LOGFLARE_ENABLED/LOGFLARE_API_BASE_URL were wired into the server env'
    ).toBeGreaterThan(0)
  })

  it('bytesChanged logged across the run matches the actual bytes stored in the backend', async () => {
    const summary = await fetchUsageLedgerSummary(ledgerUrl as string)
    const finalBytes = await getBackendTotalBytes({ prefix: usagePrefix })

    expect(finalBytes - summary.baselineBytes, JSON.stringify(summary.recentEntries, null, 2)).toBe(
      summary.totalBytesChanged
    )
  })
})
