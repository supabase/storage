import { FastifyRequest } from 'fastify'

export function generateForwardHeaders(
  forwardHeaders: string | undefined,
  request: FastifyRequest,
  ignore: string[] = []
): Record<string, string> {
  if (!forwardHeaders) {
    return {}
  }

  return forwardHeaders.split(',')
    .map(headerName => headerName.trim())
    .filter(headerName => headerName in request.headers && !ignore.includes(headerName))
    .reduce((extraHeaders, headerName) => {
      const headerValue = request.headers[headerName];
      if (typeof headerValue !== 'string') {
        throw new Error(`header ${headerName} must be string`);
      }
      extraHeaders[headerName] = headerValue;
      return extraHeaders;
    }, <Record<string, string>>{});

}
