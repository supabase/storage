import { Storage } from '@storage/storage'
import type { ValidateFunction } from 'ajv'
import Ajv from 'ajv'
import { JTDDataType } from 'ajv/dist/jtd'
import fastUri from 'fast-uri'
import { FastifyRequest } from 'fastify'
import { FromSchema, JSONSchema } from 'json-schema-to-ts'
import { default as AbortMultiPartUpload } from './commands/abort-multipart-upload'
import { default as CompleteMultipartUpload } from './commands/complete-multipart-upload'
import { default as CopyObject } from './commands/copy-object'
import { default as CreateBucket } from './commands/create-bucket'
import { default as CreateMultipartUpload } from './commands/create-multipart-upload'
import { default as DeleteBucket } from './commands/delete-bucket'
import { default as DeleteObject } from './commands/delete-object'
import { default as GetBucket } from './commands/get-bucket'
import { default as GetObject } from './commands/get-object'
import { default as HeadBucket } from './commands/head-bucket'
import { default as HeadObject } from './commands/head-object'
import { default as ListBucket } from './commands/list-buckets'
import { default as ListMultipartUploads } from './commands/list-multipart-uploads'
import { default as ListObjects } from './commands/list-objects'
import { default as ListParts } from './commands/list-parts'
import { default as PutObject } from './commands/put-object'
import { default as UploadPart } from './commands/upload-part'
import { default as UploadPartCopy } from './commands/upload-part-copy'

export type Context = {
  storage: Storage
  tenantId: string
  owner?: string
  req: FastifyRequest
  signals: { body: AbortSignal; response: AbortSignal }
}
export type S3Router = Router<Context>

const s3Commands = [
  UploadPartCopy,
  CopyObject,
  DeleteBucket,
  HeadObject,
  CreateBucket,
  CompleteMultipartUpload,
  CreateMultipartUpload,
  UploadPart,
  PutObject,
  AbortMultiPartUpload,
  ListMultipartUploads,
  DeleteObject,
  GetBucket,
  HeadBucket,
  ListBucket,
  ListParts,
  GetObject,
  ListObjects,
]

export function getRouter() {
  const router = new Router<Context>()
  s3Commands.forEach((command) => command(router))
  return router
}

export type HTTPMethod = 'get' | 'put' | 'post' | 'head' | 'delete' | 'patch'

export type Schema<
  Q extends JSONSchema = JSONSchema,
  H extends JSONSchema = JSONSchema,
  P extends JSONSchema = JSONSchema,
  B extends JSONSchema = JSONSchema,
> = {
  summary?: string
  Querystring?: Q
  Headers?: H
  Params?: P
  Body?: B
}

type ResponseType = {
  statusCode?: number
  headers?: Record<string, string>
  responseBody?: unknown
}

export type RequestInput<
  S extends Schema,
  A extends {
    [key in keyof S]: S[key] extends JSONSchema ? FromSchema<S[key]> : undefined
  } = {
    [key in keyof S]: S[key] extends JSONSchema ? FromSchema<S[key]> : undefined
  },
> = {
  Querystring: A['Querystring']
  Headers: A['Headers']
  Params: A['Params']
  Body: A['Body']
}

type Handler<Req extends Schema, Context = unknown> = (
  req: RequestInput<Req>,
  ctx: Context
) => Promise<ResponseType>

export type QuerystringMatch = {
  key: string
  value: string | undefined
}

export type RouteQuery = Record<string, string | undefined>

type Route<S extends Schema, Context> = {
  method: HTTPMethod
  type?: string
  path: string
  querystringMatches: QuerystringMatch[]
  headersMatches: string[]
  handler?: Handler<S, Context>
  schema: S
  disableContentTypeParser?: boolean
  allowEmptyJsonBody?: boolean
  acceptMultiformData?: boolean
  operation: string
  compiledSchema: () => ValidateFunction<JTDDataType<S>>
}

interface RouteOptions<S extends Schema> {
  disableContentTypeParser?: boolean
  allowEmptyJsonBody?: boolean
  acceptMultiformData?: boolean
  operation: string
  schema: S
  type?: string
}

export class Router<Context = unknown, S extends Schema = Schema> {
  protected _routes: Map<string, Route<S, Context>[]> = new Map<string, Route<S, Context>[]>()

  protected ajv = new Ajv({
    coerceTypes: 'array',
    useDefaults: true,
    removeAdditional: true,
    uriResolver: fastUri,
    addUsedSchema: false,
    allErrors: false,
  })

  registerRoute(
    method: HTTPMethod,
    url: string,
    options: RouteOptions<Schema>,
    handler: Handler<Schema, Context>
  ) {
    const { query, headers } = this.parseRequestInfo(url)
    const normalizedUrl = url.split('?')[0].split('|')[0]

    const existingPath = this._routes.get(normalizedUrl)
    const schemaToCompile: {
      Params?: JSONSchema
      Headers?: JSONSchema
      Querystring?: JSONSchema
      Body?: JSONSchema
    } = {}

    const { schema, disableContentTypeParser, allowEmptyJsonBody, acceptMultiformData, operation } =
      options

    if (schema.Params) {
      schemaToCompile.Params = schema.Params
    }
    if (schema.Body) {
      schemaToCompile.Body = schema.Body
    }
    if (schema.Headers) {
      schemaToCompile.Headers = schema.Headers
    }

    if (schema.Querystring) {
      schemaToCompile.Querystring = schema.Querystring
    }

    // If any of the keys has a required property, then the top level object is also required
    const required = Object.keys(schemaToCompile).map((key) => {
      const k = key as keyof typeof schemaToCompile
      const schemaObj = schemaToCompile[k]

      if (typeof schemaObj === 'boolean') {
        return
      }

      if (schemaObj?.required && schemaObj.required.length > 0) {
        return key as string
      }
    })

    const existingSchema = this.ajv.getSchema(method + url)

    if (!existingSchema) {
      this.ajv.addSchema(
        {
          type: 'object',
          properties: schemaToCompile,
          required: required.filter(Boolean),
        },
        method + url
      )
    }

    const newRoute: Route<Schema, Context> = {
      method: method as HTTPMethod,
      path: normalizedUrl,
      querystringMatches: query,
      headersMatches: headers,
      schema,
      compiledSchema: () =>
        this.ajv.getSchema(method + url) as ValidateFunction<JTDDataType<Schema>>,
      handler,
      disableContentTypeParser,
      allowEmptyJsonBody,
      acceptMultiformData,
      operation,
      type: options.type,
    } as const

    if (!existingPath) {
      this._routes.set(normalizedUrl, [newRoute as unknown as Route<S, Context>])
      return
    }

    existingPath.push(newRoute as unknown as Route<S, Context>)
    this._routes.set(normalizedUrl, existingPath)
  }

  // Route storage is schema-erased; the public helpers preserve per-route inference for callers.
  private registerRouteErased(
    method: HTTPMethod,
    url: string,
    options: RouteOptions<Schema>,
    handler: Handler<Schema, Context>
  ) {
    this.registerRoute(method, url, options, handler)
  }

  get<R extends Schema>(url: string, options: RouteOptions<R>, handler: Handler<R, Context>) {
    this.registerRouteErased(
      'get',
      url,
      options as unknown as RouteOptions<Schema>,
      handler as unknown as Handler<Schema, Context>
    )
  }

  post<R extends Schema>(url: string, options: RouteOptions<R>, handler: Handler<R, Context>) {
    this.registerRouteErased(
      'post',
      url,
      options as unknown as RouteOptions<Schema>,
      handler as unknown as Handler<Schema, Context>
    )
  }

  put<R extends Schema>(url: string, options: RouteOptions<R>, handler: Handler<R, Context>) {
    this.registerRouteErased(
      'put',
      url,
      options as unknown as RouteOptions<Schema>,
      handler as unknown as Handler<Schema, Context>
    )
  }

  delete<R extends Schema>(url: string, options: RouteOptions<R>, handler: Handler<R, Context>) {
    this.registerRouteErased(
      'delete',
      url,
      options as unknown as RouteOptions<Schema>,
      handler as unknown as Handler<Schema, Context>
    )
  }

  head<R extends Schema>(url: string, options: RouteOptions<R>, handler: Handler<R, Context>) {
    this.registerRouteErased(
      'head',
      url,
      options as unknown as RouteOptions<Schema>,
      handler as unknown as Handler<Schema, Context>
    )
  }

  parseQueryMatch(query: string): QuerystringMatch {
    const [key, value] = query.split('=')
    return { key, value }
  }

  parseRequestInfo(queryString: string) {
    const queries = queryString.replace(/\|.*/, '').split('?')[1]?.split('&') || []
    const headers = queryString.split('|').splice(1)

    if (queries.length === 0) {
      return { query: [{ key: '*', value: '*' }], headers }
    }
    return { query: queries.map(this.parseQueryMatch), headers }
  }

  routes() {
    return this._routes
  }

  matchRoute(
    route: Route<S, Context>,
    match: { query: RouteQuery; headers: Record<string, string>; type?: string }
  ) {
    const isOfType = match.type ? match.type === route.type : route.type === undefined

    if ((route.headersMatches?.length || 0) > 0) {
      return (
        this.matchHeaders(route.headersMatches, match.headers) &&
        this.matchQueryString(route.querystringMatches, match.query) &&
        isOfType
      )
    }

    return this.matchQueryString(route.querystringMatches, match.query) && isOfType
  }

  protected matchHeaders(headers: string[], received?: Record<string, string>) {
    if (!received) {
      return headers.length === 0
    }

    return headers.every((header) => {
      const headerParts = header.split('=')
      const headerName = headerParts[0]
      const headerValue = headerParts[1]

      const matchHeaderName = received[headerName] !== undefined
      const matchHeaderValue = headerValue ? received[headerName]?.startsWith(headerValue) : true

      return matchHeaderName && matchHeaderValue
    })
  }

  protected matchQueryString(matches: QuerystringMatch[], received?: RouteQuery) {
    let hasWildcard = false
    for (const match of matches) {
      if (match.key === '*') {
        hasWildcard = true
        break
      }
    }

    if (!received) {
      return hasWildcard
    }

    let hasReceivedQuery = false
    for (const key in received) {
      if (Object.prototype.hasOwnProperty.call(received, key)) {
        hasReceivedQuery = true
        break
      }
    }

    if (!hasReceivedQuery) {
      return hasWildcard
    }

    for (const match of matches) {
      if (match.key === '*') {
        continue
      }

      if (!Object.prototype.hasOwnProperty.call(received, match.key)) {
        return hasWildcard
      }

      if (match.value !== undefined && match.value !== received[match.key]) {
        return hasWildcard
      }
    }

    return true
  }
}

/**
 * Given a JSONSchema Definition, it returns dotted paths of all array properties
 * @param schemas
 */
export function findArrayPathsInSchemas(schemas: JSONSchema[]): string[] {
  const arrayPaths: string[] = []

  function traverse(schema: JSONSchema, currentPath = ''): void {
    if (typeof schema === 'boolean') {
      return
    }

    if (schema.type === 'array') {
      arrayPaths.push(currentPath)
    }

    if (schema.properties) {
      for (const key in schema.properties) {
        const nextSchema = schema.properties[key]
        traverse(nextSchema, currentPath ? `${currentPath}.${key}` : key)
      }
    }
  }

  schemas.forEach((schema) => traverse(schema))

  return arrayPaths
}
