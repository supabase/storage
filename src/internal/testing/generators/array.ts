export async function eachParallel<T>(times: number, fn: (index: number) => Promise<T>) {
  const promises = []
  for (let i = 0; i < times; i++) {
    promises.push(fn(i))
  }

  return Promise.all(promises)
}

export function pickRandomFromArray<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]
}

export function pickRandomRangeFromArray<T>(arr: T[], range: number): T[] {
  if (arr.length <= range) {
    return arr
  }

  const result = new Set<T>()
  while (result.size < range) {
    result.add(pickRandomFromArray(arr))
  }

  return Array.from(result)
}
