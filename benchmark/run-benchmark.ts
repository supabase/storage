import { promises as fs } from 'fs'
import path from 'path'
import { Client } from 'pg'
import { buildCallSql, SCENARIOS } from './scenarios'

const WARMUP_ITERATIONS = 1
const TIMED_ITERATIONS = 10

type DatasetState = 'pre' | 'post-default' | 'post-versioned'

interface ScenarioResult {
  name: string
  fn: string
  iterations: number
  minMs: number
  p50Ms: number
  p95Ms: number
  p99Ms: number
  maxMs: number
  rowCount: number
  explain: {
    planningTimeMs: number
    executionTimeMs: number
    sharedHitBlocks: number
    sharedReadBlocks: number
  } | null
}

function parseArgs(): { label: string; state: DatasetState } {
  const args = Object.fromEntries(
    process.argv.slice(2).map((arg) => {
      const [key, value] = arg.replace(/^--/, '').split('=')
      return [key, value]
    })
  )
  const label = args.label
  const state = args.state as DatasetState | undefined
  if (!label || !state) {
    throw new Error('Usage: run-benchmark.ts --label=<name> --state=pre|post-default|post-versioned')
  }
  if (!['pre', 'post-default', 'post-versioned'].includes(state)) {
    throw new Error(`Invalid --state=${state}`)
  }
  return { label, state }
}

function percentile(sorted: number[], p: number): number {
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))
  return sorted[idx]
}

async function timeQuery(client: Client, sql: string): Promise<{ ms: number; rows: number }> {
  const start = performance.now()
  const result = await client.query(sql)
  const ms = performance.now() - start
  return { ms, rows: result.rowCount ?? result.rows.length }
}

async function explainQuery(client: Client, sql: string) {
  const result = await client.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`)
  const plan = result.rows[0]['QUERY PLAN'][0]
  return {
    planningTimeMs: plan['Planning Time'] as number,
    executionTimeMs: plan['Execution Time'] as number,
    sharedHitBlocks: (plan.Plan?.['Shared Hit Blocks'] as number) ?? 0,
    sharedReadBlocks: (plan.Plan?.['Shared Read Blocks'] as number) ?? 0,
  }
}

async function runScenario(
  client: Client,
  scenario: (typeof SCENARIOS)[number],
  state: DatasetState
): Promise<ScenarioResult> {
  const includeVersioningArgs = state === 'post-versioned'
  const sql = buildCallSql(scenario.call, includeVersioningArgs)

  for (let i = 0; i < WARMUP_ITERATIONS; i++) {
    await timeQuery(client, sql)
  }

  const samples: number[] = []
  let rowCount = 0
  for (let i = 0; i < TIMED_ITERATIONS; i++) {
    const { ms, rows } = await timeQuery(client, sql)
    samples.push(ms)
    rowCount = rows
  }
  samples.sort((a, b) => a - b)

  let explain: ScenarioResult['explain'] = null
  try {
    explain = await explainQuery(client, sql)
  } catch (e) {
    console.warn(`  (explain failed for "${scenario.name}": ${(e as Error).message})`)
  }

  return {
    name: scenario.name,
    fn: scenario.call.fn,
    iterations: TIMED_ITERATIONS,
    minMs: samples[0],
    p50Ms: percentile(samples, 50),
    p95Ms: percentile(samples, 95),
    p99Ms: percentile(samples, 99),
    maxMs: samples[samples.length - 1],
    rowCount,
    explain,
  }
}

async function main() {
  const { label, state } = parseArgs()
  const client = new Client({ connectionString: process.env.DATABASE_URL })
  await client.connect()

  // A fresh bulk load can race autovacuum's autoanalyze - benchmarking
  // against stale/missing statistics produces a wildly bad query plan that
  // looks like a catastrophic regression but is actually just this table
  // never having been analyzed yet. The seed scripts already do this, but
  // guarantee it here too regardless of how the table was populated.
  console.log('Running ANALYZE storage.objects before benchmarking...')
  await client.query('ANALYZE storage.objects')

  const applicable = SCENARIOS.filter((s) => s.appliesTo.includes(state))
  console.log(`Running ${applicable.length} scenarios against state="${state}" (label="${label}")`)

  const results: ScenarioResult[] = []
  try {
    for (const scenario of applicable) {
      process.stdout.write(`  ${scenario.name} ... `)
      const result = await runScenario(client, scenario, state)
      results.push(result)
      console.log(`p50=${result.p50Ms.toFixed(2)}ms p95=${result.p95Ms.toFixed(2)}ms rows=${result.rowCount}`)
    }
  } finally {
    await client.end()
  }

  const outDir = path.resolve(__dirname, 'results')
  await fs.mkdir(outDir, { recursive: true })
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  const outFile = path.join(outDir, `${label}-${timestamp}.json`)
  await fs.writeFile(outFile, JSON.stringify({ label, state, timestamp, results }, null, 2))
  console.log(`\nResults written to ${outFile}`)
}

void main()
