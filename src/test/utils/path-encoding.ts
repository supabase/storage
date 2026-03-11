export function encodePathPreservingSeparatorsForTest(path: string): string {
  return path
    .split('/')
    .map((pathToken) => encodeURIComponent(pathToken))
    .join('/')
}

export function encodeBucketAndObjectPathForTest(bucket: string, key: string): string {
  return `${encodeURIComponent(bucket)}/${encodePathPreservingSeparatorsForTest(key)}`
}
