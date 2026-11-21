import { getAcceptanceConfig } from './config'
import { checkpointUsageBytes, drainUsageLedgerViolations } from './usage-ledger'

afterEach(async () => {
  const { usageLedgerUrl: ledgerUrl } = getAcceptanceConfig()
  if (!ledgerUrl) {
    return
  }

  await checkpointUsageBytes(expect.getState().currentTestName ?? 'afterEach')

  const violations = await drainUsageLedgerViolations(ledgerUrl)
  expect(violations, JSON.stringify(violations, null, 2)).toEqual([])
})
