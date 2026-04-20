import {
  Function as PprofFunction,
  Line as PprofLine,
  Location as PprofLocation,
  Mapping as PprofMapping,
  Sample as PprofSample,
  ValueType as PprofValueType,
  Profile,
  StringTable,
} from 'pprof-format'
import { mergeProfiles, mergeStoppedProfileBuffers } from './profile'

function toArrayBuffer(value: Uint8Array<ArrayBufferLike>) {
  const copy = new Uint8Array(value.byteLength)
  copy.set(value)
  return copy.buffer
}

function buildProfile(options: {
  functionName: string
  sampleValue: number
  comment?: string
  defaultSampleTypeName?: string
  omitTiming?: boolean
  locationMappingId?: number
  lineFunctionId?: number
  locationId?: number
  locationIdsInSample?: number[]
  periodTypeName?: string
  sampleTypeName?: string
  timeNanos?: bigint
  durationNanos?: bigint
}) {
  const stringTable = new StringTable()
  const sampleTypeName = options.sampleTypeName ?? 'samples'
  const periodTypeName = options.periodTypeName ?? sampleTypeName
  const defaultSampleTypeName = options.defaultSampleTypeName ?? sampleTypeName
  const sampleType = new PprofValueType({
    type: stringTable.dedup(sampleTypeName),
    unit: stringTable.dedup('count'),
  })
  const periodType = new PprofValueType({
    type: stringTable.dedup(periodTypeName),
    unit: stringTable.dedup('count'),
  })
  const locationId = options.locationId ?? 1

  return new Profile({
    stringTable,
    sampleType: [sampleType],
    periodType,
    period: 1,
    ...(options.omitTiming
      ? {}
      : {
          timeNanos: options.timeNanos ?? 1n,
          durationNanos: options.durationNanos ?? 1_000_000_000n,
        }),
    mapping: [
      new PprofMapping({
        id: 1,
        hasFunctions: true,
        hasFilenames: true,
        hasLineNumbers: true,
      }),
    ],
    function: [
      new PprofFunction({
        id: 1,
        name: stringTable.dedup(options.functionName),
        systemName: stringTable.dedup(options.functionName),
        filename: stringTable.dedup(`${options.functionName}.ts`),
        startLine: 1,
      }),
    ],
    location: [
      new PprofLocation({
        id: locationId,
        mappingId: options.locationMappingId ?? 1,
        line: [new PprofLine({ functionId: options.lineFunctionId ?? 1, line: 1 })],
      }),
    ],
    sample: [
      new PprofSample({
        locationId: options.locationIdsInSample ?? [locationId],
        value: [options.sampleValue],
      }),
    ],
    defaultSampleType: stringTable.dedup(defaultSampleTypeName),
    comment: options.comment ? [stringTable.dedup(options.comment)] : [],
  })
}

describe('mergeProfiles', () => {
  it('merges compatible profiles and preserves the latest timing metadata', () => {
    const merged = mergeProfiles([
      buildProfile({
        functionName: 'workerA',
        sampleValue: 3,
        comment: 'shared comment',
        timeNanos: 1n,
        durationNanos: 2n,
      }),
      buildProfile({
        functionName: 'workerB',
        sampleValue: 5,
        comment: 'shared comment',
        timeNanos: 9n,
        durationNanos: 7n,
      }),
    ])

    const decoded = Profile.decode(merged)
    expect(decoded.sample.map((sample) => sample.value[0])).toEqual([3, 5])
    expect(decoded.function).toHaveLength(2)
    expect(decoded.comment).toHaveLength(1)
    expect(decoded.timeNanos).toBe(9)
    expect(decoded.durationNanos).toBe(7)
  })

  it('treats missing timing metadata as zero when picking the latest profile timing', () => {
    const merged = mergeProfiles([
      buildProfile({
        functionName: 'workerA',
        sampleValue: 3,
        omitTiming: true,
      }),
      buildProfile({
        functionName: 'workerB',
        sampleValue: 5,
        timeNanos: 9n,
        durationNanos: 7n,
      }),
    ])

    const decoded = Profile.decode(merged)
    expect(decoded.timeNanos).toBe(9)
    expect(decoded.durationNanos).toBe(7)
  })

  it('rejects profiles with mismatched sample types', () => {
    expect(() =>
      mergeProfiles([
        buildProfile({ functionName: 'workerA', sampleValue: 1, sampleTypeName: 'samples' }),
        buildProfile({ functionName: 'workerB', sampleValue: 2, sampleTypeName: 'bytes' }),
      ])
    ).toThrow('Cannot merge pprof profiles with different sample types.')
  })

  it('rejects profiles with mismatched period types', () => {
    expect(() =>
      mergeProfiles([
        buildProfile({ functionName: 'workerA', sampleValue: 1, periodTypeName: 'samples' }),
        buildProfile({ functionName: 'workerB', sampleValue: 2, periodTypeName: 'bytes' }),
      ])
    ).toThrow('Cannot merge pprof profiles with different period types.')
  })

  it('rejects profiles with mismatched default sample types', () => {
    expect(() =>
      mergeProfiles([
        buildProfile({
          functionName: 'workerA',
          sampleValue: 1,
          defaultSampleTypeName: 'samples',
        }),
        buildProfile({
          functionName: 'workerB',
          sampleValue: 2,
          defaultSampleTypeName: 'bytes',
        }),
      ])
    ).toThrow('Cannot merge pprof profiles with different default sample types.')
  })

  it('rejects profiles whose samples reference unknown location ids', () => {
    expect(() =>
      mergeProfiles([
        buildProfile({ functionName: 'workerA', sampleValue: 1 }),
        buildProfile({
          functionName: 'workerB',
          sampleValue: 2,
          locationIdsInSample: [99],
        }),
      ])
    ).toThrow('Cannot merge pprof profiles with unknown location id: 99.')
  })

  it('rejects profiles whose samples use zero location ids', () => {
    expect(() =>
      mergeProfiles([
        buildProfile({ functionName: 'workerA', sampleValue: 1 }),
        buildProfile({
          functionName: 'workerB',
          sampleValue: 2,
          locationIdsInSample: [0],
        }),
      ])
    ).toThrow('Cannot merge pprof profiles with unknown location id: 0.')
  })

  it('rejects profiles whose locations reference unknown mapping ids', () => {
    expect(() =>
      mergeProfiles([
        buildProfile({ functionName: 'workerA', sampleValue: 1 }),
        buildProfile({
          functionName: 'workerB',
          sampleValue: 2,
          locationMappingId: 99,
        }),
      ])
    ).toThrow('Cannot merge pprof profiles with unknown mapping id: 99.')
  })

  it('rejects profiles whose lines reference unknown function ids', () => {
    expect(() =>
      mergeProfiles([
        buildProfile({ functionName: 'workerA', sampleValue: 1 }),
        buildProfile({
          functionName: 'workerB',
          sampleValue: 2,
          lineFunctionId: 99,
        }),
      ])
    ).toThrow('Cannot merge pprof profiles with unknown function id: 99.')
  })
})

describe('mergeStoppedProfileBuffers', () => {
  it('merges fulfilled stop buffers and drops empty buffers', () => {
    const merged = mergeStoppedProfileBuffers([
      {
        status: 'fulfilled',
        value: toArrayBuffer(buildProfile({ functionName: 'workerA', sampleValue: 3 }).encode()),
      },
      {
        status: 'fulfilled',
        value: new ArrayBuffer(0),
      },
      {
        status: 'fulfilled',
        value: toArrayBuffer(buildProfile({ functionName: 'workerB', sampleValue: 5 }).encode()),
      },
    ])

    const decoded = Profile.decode(merged)
    expect(decoded.sample.map((sample) => sample.value[0])).toEqual([3, 5])
  })

  it('throws the first rejected stop error', () => {
    expect(() =>
      mergeStoppedProfileBuffers([
        {
          status: 'rejected',
          reason: new Error('stop failed'),
        },
      ])
    ).toThrow('stop failed')
  })
})
