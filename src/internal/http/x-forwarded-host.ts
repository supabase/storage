interface XForwardedHostRegExpConfig {
  isMultitenant: boolean
  requestXForwardedHostRegExp?: string
}

let cachedPattern: string | undefined
let cachedRegExp: RegExp | undefined

export function getXForwardedHostRegExp({
  isMultitenant,
  requestXForwardedHostRegExp,
}: XForwardedHostRegExpConfig): RegExp | undefined {
  if (!isMultitenant || !requestXForwardedHostRegExp) {
    return undefined
  }

  if (requestXForwardedHostRegExp !== cachedPattern) {
    const nextRegExp = new RegExp(requestXForwardedHostRegExp)

    cachedPattern = requestXForwardedHostRegExp
    cachedRegExp = nextRegExp
  }

  return cachedRegExp
}
