export function encodeCopySource(bucket: string, key: string): string {
  return `${encodeURIComponent(bucket)}/${key
    .split('/')
    .map((pathToken) => encodeURIComponent(pathToken))
    .join('/')}`
}
