import accepts from '@fastify/accepts'
import { FastifyInstance } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import xml from 'xml2js'

type XmlParserOptions = { disableContentParser?: boolean; parseAsArray?: string[] }
type RequestError = Error & { statusCode?: number }

function isValidXmlCodePoint(codePoint: number): boolean {
  if (!Number.isInteger(codePoint) || codePoint < 0 || codePoint > 0x10ffff) {
    return false
  }

  return (
    codePoint === 0x9 ||
    codePoint === 0xa ||
    codePoint === 0xd ||
    (codePoint >= 0x20 && codePoint <= 0xd7ff) ||
    (codePoint >= 0xe000 && codePoint <= 0xfffd) ||
    (codePoint >= 0x10000 && codePoint <= 0x10ffff)
  )
}

function getInvalidXmlNumericEntity(value: string): string | undefined {
  const numericEntityPattern = /&#([xX][0-9a-fA-F]{1,6}|[0-9]{1,7});/g

  let match = numericEntityPattern.exec(value)
  while (match) {
    const rawValue = match[1]
    const isHex = rawValue[0].toLowerCase() === 'x'
    const codePoint = Number.parseInt(isHex ? rawValue.slice(1) : rawValue, isHex ? 16 : 10)

    if (!isValidXmlCodePoint(codePoint)) {
      return match[0]
    }

    match = numericEntityPattern.exec(value)
  }
}

function forcePathAsArray(node: unknown, pathSegments: string[]): void {
  if (pathSegments.length === 0 || node === null || node === undefined) {
    return
  }

  if (Array.isArray(node)) {
    node.forEach((item) => forcePathAsArray(item, pathSegments))
    return
  }

  if (typeof node !== 'object') {
    return
  }

  const [current, ...rest] = pathSegments
  const currentRecord = node as Record<string, unknown>

  if (!(current in currentRecord)) {
    return
  }

  if (rest.length === 0) {
    const value = currentRecord[current]
    if (value !== undefined && !Array.isArray(value)) {
      currentRecord[current] = [value]
    }
    return
  }

  forcePathAsArray(currentRecord[current], rest)
}

export const xmlParser = fastifyPlugin(
  async function (fastify: FastifyInstance, opts: XmlParserOptions) {
    fastify.register(accepts)

    if (!opts.disableContentParser) {
      fastify.addContentTypeParser(
        ['text/xml', 'application/xml'],
        { parseAs: 'string' },
        (_request, body, done) => {
          if (!body) {
            done(null, null)
            return
          }

          const xmlBody = typeof body === 'string' ? body : body.toString('utf8')
          const invalidNumericEntity = getInvalidXmlNumericEntity(xmlBody)
          if (invalidNumericEntity) {
            const parseError: RequestError = new Error(
              `Invalid XML payload: invalid numeric entity ${invalidNumericEntity}`
            )
            parseError.statusCode = 400
            done(parseError)
            return
          }

          xml.parseString(
            xmlBody,
            {
              explicitArray: false,
              trim: true,
              valueProcessors: [xml.processors.parseNumbers, xml.processors.parseBooleans],
            },
            (err: Error | null, parsed: unknown) => {
              if (err) {
                const parseError: RequestError = new Error(`Invalid XML payload: ${err.message}`)
                parseError.statusCode = 400
                done(parseError)
                return
              }

              if (parsed && opts.parseAsArray?.length) {
                opts.parseAsArray.forEach((path) => {
                  if (!path) {
                    return
                  }
                  forcePathAsArray(parsed, path.split('.'))
                })
              }

              done(null, parsed)
            }
          )
        }
      )
    }

    fastify.addHook('preSerialization', async (req, res, payload) => {
      const accept = req.accepts()

      const acceptedTypes = ['application/xml', 'text/html']

      if (acceptedTypes.some((allowed) => accept.types(acceptedTypes) === allowed)) {
        res.serializer((payload) => payload)

        const xmlBuilder = new xml.Builder({
          renderOpts: {
            pretty: false,
          },
        })
        const xmlPayload = xmlBuilder.buildObject(payload)
        res.type('application/xml')
        res.header('content-type', 'application/xml; charset=utf-8')
        return xmlPayload
      }

      return payload
    })
  },
  { name: 'xml-parser' }
)
