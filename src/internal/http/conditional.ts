const HTTP_MONTHS: Record<string, number> = {
  Jan: 0,
  Feb: 1,
  Mar: 2,
  Apr: 3,
  May: 4,
  Jun: 5,
  Jul: 6,
  Aug: 7,
  Sep: 8,
  Oct: 9,
  Nov: 10,
  Dec: 11,
}

// RFC 9110 5.6.7 HTTP-date formats: IMF-fixdate, obsolete RFC 850, and ANSI C asctime().
const IMF_FIXDATE =
  /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), (\d{2}) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT$/
const RFC850_DATE =
  /^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), (\d{2})-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2}) (\d{2}):(\d{2}):(\d{2}) GMT$/
const ASCTIME_DATE =
  /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ( \d|\d{2}) (\d{2}):(\d{2}):(\d{2}) (\d{4})$/

export function parseHttpDate(value: string, now = new Date()): Date | undefined {
  const trimmed = value.trim()
  let year: number
  let month: number
  let day: number
  let hour: number
  let minute: number
  let second: number

  let isRfc850 = false
  const imf = IMF_FIXDATE.exec(trimmed)
  if (imf) {
    day = Number(imf[1])
    month = HTTP_MONTHS[imf[2]]
    year = Number(imf[3])
    hour = Number(imf[4])
    minute = Number(imf[5])
    second = Number(imf[6])
  } else {
    const rfc850 = RFC850_DATE.exec(trimmed)
    if (rfc850) {
      isRfc850 = true
      day = Number(rfc850[1])
      month = HTTP_MONTHS[rfc850[2]]
      year = Math.floor(now.getUTCFullYear() / 100) * 100 + Number(rfc850[3])
      hour = Number(rfc850[4])
      minute = Number(rfc850[5])
      second = Number(rfc850[6])
    } else {
      const asctime = ASCTIME_DATE.exec(trimmed)
      if (!asctime) {
        return undefined
      }
      month = HTTP_MONTHS[asctime[1]]
      day = Number(asctime[2])
      hour = Number(asctime[3])
      minute = Number(asctime[4])
      second = Number(asctime[5])
      year = Number(asctime[6])
    }
  }

  // RFC 9110 5.6.7 permits leap second 60; clamp to 59 for POSIX timestamps without day rollover.
  if (hour > 23 || minute > 59 || second > 60) {
    return undefined
  }
  const clampedSecond = Math.min(second, 59)

  if (isRfc850) {
    const fiftyYearsAhead = Date.UTC(
      now.getUTCFullYear() + 50,
      now.getUTCMonth(),
      now.getUTCDate(),
      now.getUTCHours(),
      now.getUTCMinutes(),
      now.getUTCSeconds(),
      now.getUTCMilliseconds()
    )
    if (Date.UTC(year, month, day, hour, minute, clampedSecond) > fiftyYearsAhead) {
      year -= 100
    } else if (Date.UTC(year + 100, month, day, hour, minute, clampedSecond) <= fiftyYearsAhead) {
      year += 100
    }
  }

  const date = new Date(Date.UTC(year, month, day, hour, minute, clampedSecond))
  if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month || date.getUTCDate() !== day) {
    return undefined
  }
  return date
}

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
