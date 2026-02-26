import { decodeXmlNumericEntities } from '../http/plugins/xml'

describe('decodeXmlNumericEntities', () => {
  test('decodes hexadecimal entities including astral code points', () => {
    expect(decodeXmlNumericEntities('a&#x1f642;b')).toBe('a🙂b')
  })

  test('decodes decimal entities', () => {
    expect(decodeXmlNumericEntities('a&#128578;b')).toBe('a🙂b')
  })

  test('keeps out-of-range entities unchanged', () => {
    expect(decodeXmlNumericEntities('a&#x110000;b')).toBe('a&#x110000;b')
  })
})
