import { getSslSettings, isIpAddress } from '@internal/database/util'

describe('database utils', () => {
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
    ['2406%3Ada18%3A4fd%3A9b09%3A2c76%3A5d38%3Ade30%3A7904', true],
  ])('is %s ip address, expected %s', (text: string, expected: boolean) => {
    expect(isIpAddress(text)).toBe(expected)
  })

  describe('getSslSettings', () => {
    test('should return no SSL settings if database root CA not set', () => {
      expect(
        getSslSettings({ connectionString: 'foobar', databaseSSLRootCert: undefined })
      ).toBeUndefined()
    })

    test('should return SSL settings if hostname is an IP address', () => {
      expect(
        getSslSettings({
          connectionString: 'postgres://foo:bar@1.2.3.4:5432/postgres',
          databaseSSLRootCert: '<cert>',
        })
      ).toStrictEqual({ ca: '<cert>', rejectUnauthorized: false })
    })

    test('should return SSL settings if hostname is not an IP address', () => {
      expect(
        getSslSettings({
          connectionString: 'postgres://foo:bar@db.ref.supabase.red:5432/postgres',
          databaseSSLRootCert: '<cert>',
        })
      ).toStrictEqual({ ca: '<cert>' })
    })

    test('should return SSL settings if hostname is not parseable', () => {
      expect(
        getSslSettings({
          connectionString: 'postgres://foo:bar@db.ref.supabase."red:5432/postgres',
          databaseSSLRootCert: '<cert>',
        })
      ).toStrictEqual({ ca: '<cert>' })
    })
  })
})
