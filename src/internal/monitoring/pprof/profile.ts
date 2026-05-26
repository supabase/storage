import {
  Function as PprofFunction,
  Label as PprofLabel,
  Line as PprofLine,
  Location as PprofLocation,
  Mapping as PprofMapping,
  Sample as PprofSample,
  ValueType as PprofValueType,
  Profile,
  StringTable,
} from 'pprof-format'

function numericKey(value: number | bigint) {
  return value.toString()
}

function numericToBigInt(value: number | bigint | undefined) {
  if (typeof value === 'bigint') {
    return value
  }

  if (typeof value === 'number') {
    return BigInt(value)
  }

  return 0n
}

function maxNumeric(values: Array<number | bigint | undefined>) {
  let max = 0n

  for (const value of values) {
    const numeric = numericToBigInt(value)
    if (numeric > max) {
      max = numeric
    }
  }

  return max
}

function getString(profile: Profile, index: number | bigint | undefined) {
  const numericIndex = Number(index ?? 0)
  return profile.stringTable.strings[numericIndex] ?? ''
}

function getValueTypeSignature(profile: Profile, valueType?: PprofValueType) {
  if (!valueType) {
    return ''
  }

  return `${getString(profile, valueType.type)}:${getString(profile, valueType.unit)}`
}

function getDefaultSampleTypeSignature(profile: Profile) {
  return getString(profile, profile.defaultSampleType)
}

function buildStringIndexMap(profile: Profile, stringTable: StringTable) {
  return profile.stringTable.strings.map((entry) => stringTable.dedup(entry))
}

function buildProfileMetadataFromFirstProfile(
  profile: Profile,
  stringIndexMap: number[],
  stringTable: StringTable
) {
  return {
    sampleType: profile.sampleType.map(
      (valueType) =>
        new PprofValueType({
          type: stringIndexMap[Number(valueType.type)] ?? 0,
          unit: stringIndexMap[Number(valueType.unit)] ?? 0,
        })
    ),
    periodType: profile.periodType
      ? new PprofValueType({
          type: stringIndexMap[Number(profile.periodType.type)] ?? 0,
          unit: stringIndexMap[Number(profile.periodType.unit)] ?? 0,
        })
      : undefined,
    dropFrames: stringIndexMap[Number(profile.dropFrames)] ?? 0,
    keepFrames: stringIndexMap[Number(profile.keepFrames)] ?? 0,
    defaultSampleType: stringIndexMap[Number(profile.defaultSampleType)] ?? 0,
    stringTable,
  }
}

function getRequiredMappedId(
  ids: Map<string, number>,
  value: number | bigint | undefined,
  referenceType: 'function' | 'mapping' | 'location',
  options: {
    allowZero: boolean
  }
) {
  const numericValue = numericToBigInt(value)
  if (options.allowZero && numericValue === 0n) {
    return 0
  }

  const mappedId = ids.get(numericKey(numericValue))
  if (mappedId === undefined) {
    throw new Error(
      `Cannot merge pprof profiles with unknown ${referenceType} id: ${numericValue.toString()}.`
    )
  }

  return mappedId
}

function validateMergeCompatibility(referenceProfile: Profile, profile: Profile) {
  if (referenceProfile.sampleType.length !== profile.sampleType.length) {
    throw new Error('Cannot merge pprof profiles with different sample type counts.')
  }

  for (let index = 0; index < referenceProfile.sampleType.length; index += 1) {
    if (
      getValueTypeSignature(referenceProfile, referenceProfile.sampleType[index]) !==
      getValueTypeSignature(profile, profile.sampleType[index])
    ) {
      throw new Error('Cannot merge pprof profiles with different sample types.')
    }
  }

  if (
    getValueTypeSignature(referenceProfile, referenceProfile.periodType) !==
    getValueTypeSignature(profile, profile.periodType)
  ) {
    throw new Error('Cannot merge pprof profiles with different period types.')
  }

  if (getDefaultSampleTypeSignature(referenceProfile) !== getDefaultSampleTypeSignature(profile)) {
    throw new Error('Cannot merge pprof profiles with different default sample types.')
  }
}

export function mergeProfiles(profiles: Profile[]) {
  if (profiles.length === 0) {
    return Buffer.alloc(0)
  }

  if (profiles.length === 1) {
    return Buffer.from(profiles[0].encode())
  }

  const stringTable = new StringTable()
  const referenceProfile = profiles[0]
  const mergedComments = new Set<number>()
  const firstStringIndexMap = buildStringIndexMap(referenceProfile, stringTable)
  const mergedProfile = new Profile({
    ...buildProfileMetadataFromFirstProfile(referenceProfile, firstStringIndexMap, stringTable),
    sample: [],
    mapping: [],
    location: [],
    function: [],
    comment: [],
    timeNanos: maxNumeric(profiles.map((profile) => profile.timeNanos)),
    durationNanos: maxNumeric(profiles.map((profile) => profile.durationNanos)),
    period: referenceProfile.period,
  })

  for (let profileIndex = 0; profileIndex < profiles.length; profileIndex += 1) {
    const profile = profiles[profileIndex]
    if (profileIndex > 0) {
      validateMergeCompatibility(referenceProfile, profile)
    }

    const stringIndexMap =
      profileIndex === 0 ? firstStringIndexMap : buildStringIndexMap(profile, stringTable)
    const mappingIds = new Map<string, number>()
    const functionIds = new Map<string, number>()
    const locationIds = new Map<string, number>()

    for (const mapping of profile.mapping) {
      const nextId = mergedProfile.mapping.length + 1
      mappingIds.set(numericKey(mapping.id), nextId)
      mergedProfile.mapping.push(
        new PprofMapping({
          id: nextId,
          memoryStart: mapping.memoryStart,
          memoryLimit: mapping.memoryLimit,
          fileOffset: mapping.fileOffset,
          filename: stringIndexMap[Number(mapping.filename)] ?? 0,
          buildId: stringIndexMap[Number(mapping.buildId)] ?? 0,
          hasFunctions: mapping.hasFunctions,
          hasFilenames: mapping.hasFilenames,
          hasLineNumbers: mapping.hasLineNumbers,
          hasInlineFrames: mapping.hasInlineFrames,
        })
      )
    }

    for (const fn of profile.function) {
      const nextId = mergedProfile.function.length + 1
      functionIds.set(numericKey(fn.id), nextId)
      mergedProfile.function.push(
        new PprofFunction({
          id: nextId,
          name: stringIndexMap[Number(fn.name)] ?? 0,
          systemName: stringIndexMap[Number(fn.systemName)] ?? 0,
          filename: stringIndexMap[Number(fn.filename)] ?? 0,
          startLine: fn.startLine,
        })
      )
    }

    for (const location of profile.location) {
      const nextId = mergedProfile.location.length + 1
      locationIds.set(numericKey(location.id), nextId)
      mergedProfile.location.push(
        new PprofLocation({
          id: nextId,
          mappingId: getRequiredMappedId(mappingIds, location.mappingId, 'mapping', {
            allowZero: true,
          }),
          address: location.address,
          isFolded: location.isFolded,
          line: location.line.map(
            (line) =>
              new PprofLine({
                functionId: getRequiredMappedId(functionIds, line.functionId, 'function', {
                  allowZero: true,
                }),
                line: line.line,
              })
          ),
        })
      )
    }

    for (const sample of profile.sample) {
      mergedProfile.sample.push(
        new PprofSample({
          locationId: sample.locationId.map((locationId) =>
            getRequiredMappedId(locationIds, locationId, 'location', {
              allowZero: false,
            })
          ),
          value: [...sample.value],
          label: sample.label.map(
            (label) =>
              new PprofLabel({
                key: stringIndexMap[Number(label.key)] ?? 0,
                str: stringIndexMap[Number(label.str)] ?? 0,
                num: label.num,
                numUnit: stringIndexMap[Number(label.numUnit)] ?? 0,
              })
          ),
        })
      )
    }

    for (const comment of profile.comment) {
      const mappedComment = stringIndexMap[Number(comment)] ?? 0
      if (!mergedComments.has(mappedComment)) {
        mergedComments.add(mappedComment)
        mergedProfile.comment.push(mappedComment)
      }
    }
  }

  return Buffer.from(mergedProfile.encode())
}

export function mergeStoppedProfileBuffers(stopResults: PromiseSettledResult<ArrayBuffer>[]) {
  const failedStop = stopResults.find((result) => result.status === 'rejected')
  if (failedStop?.status === 'rejected') {
    throw failedStop.reason
  }

  const profiles = stopResults
    .filter(
      (result): result is PromiseFulfilledResult<ArrayBuffer> => result.status === 'fulfilled'
    )
    .map((result) => new Uint8Array(result.value))
    .filter((result) => result.byteLength > 0)
    .map((result) => Profile.decode(result))

  return mergeProfiles(profiles)
}
