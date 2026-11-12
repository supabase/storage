import { describeAdapterConformance } from '../conformance'
import { memory } from './index'

// The reference adapter claims the full contract: every capability at its default.
describeAdapterConformance({
  name: 'memory',
  create: () => memory(),
  capabilities: { timing: { livenessMs: 200, waitMs: 3_000, quietMs: 30, delayMs: 150 } },
})
