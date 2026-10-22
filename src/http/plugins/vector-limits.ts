export function resolveTenantVectorLimits(
  tenantLimits: { maxBuckets?: number; maxIndexes?: number } | undefined,
  defaults: { maxBuckets: number; maxIndexes: number }
) {
  return {
    maxBucketCount: tenantLimits?.maxBuckets ?? defaults.maxBuckets,
    maxIndexCount: tenantLimits?.maxIndexes ?? defaults.maxIndexes,
  }
}
