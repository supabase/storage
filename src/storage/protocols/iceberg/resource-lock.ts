export function icebergResourceLockKey(resourceType: string, resourceId: string): string {
  return `${resourceType}:${resourceId}`
}
