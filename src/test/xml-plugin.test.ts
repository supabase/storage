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

  test('keeps NUL entities unchanged', () => {
    expect(decodeXmlNumericEntities('a&#0;b')).toBe('a&#0;b')
    expect(decodeXmlNumericEntities('a&#000;b')).toBe('a&#000;b')
    expect(decodeXmlNumericEntities('a&#x0;b')).toBe('a&#x0;b')
  })

  test('keeps surrogate-half entities unchanged', () => {
    expect(decodeXmlNumericEntities('a&#xD800;b')).toBe('a&#xD800;b')
    expect(decodeXmlNumericEntities('a&#55296;b')).toBe('a&#55296;b')
  })

  test('keeps noncharacter entities unchanged', () => {
    expect(decodeXmlNumericEntities('a&#xFFFF;b')).toBe('a&#xFFFF;b')
    expect(decodeXmlNumericEntities('a&#65535;b')).toBe('a&#65535;b')
  })
})
