// biome-ignore lint/suspicious/noControlCharactersInRegex: XML 1.0 excludes these ranges.
const INVALID_XML_CHARACTER = /[\u0000-\u0008\u000B\u000C\u000E-\u001F\uFFFE\uFFFF]/

export function hasInvalidXmlCharacters(value: string): boolean {
  return INVALID_XML_CHARACTER.test(value) || !value.isWellFormed()
}

/**
 * Sibling elements whose relative order must survive serialization, such as
 * the interleaved <Version> and <DeleteMarker> entries of ListVersionsResult.
 * A plain object groups repeated tags by key, so the XML response serializer
 * expands a document child holding an XmlSequence in place into its entries,
 * in order.
 */
export class XmlSequence {
  constructor(readonly entries: [tag: string, value: unknown][]) {}
}
