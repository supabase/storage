import { isIpAddress } from '@internal/database'

describe('isIpAddress', () => {
  test.each([
    ['', false],
    ['test', false],
    ['db.foobar.supabase.red', false],
    ['5.5.5.a', false],
    ['5.5.5.5', true],
    ['121.212.187.123', true],
    ['121.212.187.5', true],
    ['2001:db8:3333:4444:5555:6666:7777:8888', true],
    ['2001:db8:3333:4444:CCCC:DDDD:EEEE:FFFF', true],
  ])('is %s ip address, expected %s', (text: string, expected: boolean) => {
    expect(isIpAddress(text)).toBe(expected)
  })
})
