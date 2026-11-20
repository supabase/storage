// HTTP dates have one-second precision; invalid dates yield NaN (comparisons are false).
export function toSeconds(date: Date) {
  return Math.floor(date.getTime() / 1000)
}

export function unquoteETag(value: string) {
  return value
    .trim()
    .replace(/^W\//, '')
    .replace(/^"(.*)"$/, '$1')
}

export function matchesETag(
  condition: string,
  eTag: string,
  { allowWildcard = true }: { allowWildcard?: boolean } = {}
) {
  const target = unquoteETag(eTag)

  // RFC 9110 8.8.3: commas are valid inside a quoted entity-tag
  // so split on commas outside quotes only.
  return (condition.match(/(?:"[^"]*"|[^,])+/g) ?? []).some((candidate) => {
    const value = candidate.trim()
    return value.length > 0 && ((allowWildcard && value === '*') || unquoteETag(value) === target)
  })
}
