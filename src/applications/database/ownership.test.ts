// @ts-expect-error: Platformatic ships this module without type declarations.
import { pickByAffinity as platformaticPickByAffinity } from '@platformatic/runtime/lib/worker/affinity.js'
import { describe, expect, it } from 'vitest'
import { DestinationOwnership, pickByAffinity } from './ownership.js'

function workerIds(count: number) {
  return Array.from({ length: count }, (_, i) => `database:${i}`)
}

describe('Database worker destination ownership', () => {
  it('picks the same worker as Platformatic messaging affinity', () => {
    for (let count = 1; count <= 8; count++) {
      const ids = workerIds(count)
      const workers = ids.map((id) => ({ id }))
      for (let i = 0; i < 2_000; i++) {
        const key = `tenant-${i.toString(36)}-${count}`
        expect(pickByAffinity(ids, key)).toBe(platformaticPickByAffinity(workers, key).id)
      }
    }
  })

  it('gives every destination exactly one owner among the configured workers', () => {
    const workers = workerIds(4).map((id) => new DestinationOwnership({ id, count: 4 }))

    for (let i = 0; i < 500; i++) {
      const destination = `tenant-${i}`
      expect(workers.filter((worker) => worker.owns(destination))).toHaveLength(1)
    }
  })

  it('keeps ownership on the configured workers while one of them is down', () => {
    // Platformatic would route a key owned by database:0 elsewhere while database:0 is
    // restarting; the worker it lands on must still refuse it.
    const destination = Array.from({ length: 100 }, (_, i) => `tenant-${i}`).find(
      (id) => pickByAffinity(workerIds(3), id) === 'database:0'
    ) as string
    const fallback = pickByAffinity(['database:1', 'database:2'], destination) as string

    expect(new DestinationOwnership({ id: fallback, count: 3 }).owns(destination)).toBe(false)
    expect(new DestinationOwnership({ id: 'database:0', count: 3 }).owns(destination)).toBe(true)
  })

  it('owns every destination with a single worker or outside Watt', () => {
    expect(new DestinationOwnership({ id: 'database:0', count: 1 }).owns('tenant-a')).toBe(true)
    expect(new DestinationOwnership(undefined).owns('tenant-a')).toBe(true)
    expect(new DestinationOwnership(undefined).ownerOf('tenant-a')).toBeUndefined()
  })
})
