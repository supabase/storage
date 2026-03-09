export function encodePathPreservingSeparators(path: string): string {
  return path
    .split('/')
    .map((pathToken) => encodeURIComponent(pathToken))
    .join('/')
}

export function encodeBucketAndObjectPath(bucket: string, key: string): string {
  return `${encodeURIComponent(bucket)}/${encodePathPreservingSeparators(key)}`
}
