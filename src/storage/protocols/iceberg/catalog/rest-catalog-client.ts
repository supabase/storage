import { ERRORS } from '@internal/errors'
import { type SignRequestOptions, signRequest } from 'aws-sigv4-sign'
import JSONBigint from 'json-bigint'
import {
  createAlreadyExistsError,
  createAuthenticationTimeoutError,
  createBadRequestError,
  createForbiddenError,
  createInternalServerError,
  createNoSuchNamespaceError,
  createNoSuchTableError,
  createSlowDownError,
  createUnauthorizedError,
  createUnprocessableEntityError,
  createUnsupportedOperationError,
  IcebergError,
  IcebergHttpStatusCode,
} from './errors'

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000

export type QueryParamPrimitive = string | number | boolean
export type QueryParamValue =
  | QueryParamPrimitive
  | ReadonlyArray<QueryParamPrimitive | null | undefined>
  | null
  | undefined
type NotFoundResource = 'namespace' | 'table'

export interface GetConfigRequest {
  tenantId?: string
  warehouse: string
}

export interface GetConfigResponse {
  overrides?: {
    warehouse?: string
    prefix?: string
  }
  defaults?: {
    clients?: string
  }
  endpoints?: string[]
}

export interface ListNamespacesRequest {
  warehouse: string
  pageToken?: string
  pageSize?: number
  parent?: string
}

export interface ListNamespacesResponse {
  namespaces: string[][]
  'next-page-token'?: string
}

export interface FetchRequestConfig {
  method: string
  url: string
  headers: Headers
  body?: string
}

type FetchRequestInput = {
  method?: string
  url: string
  params?: Record<string, QueryParamValue>
  data?: unknown
  headers?: Record<string, string>
  notFoundResource?: NotFoundResource
}

interface CatalogAuth {
  authorize(req: FetchRequestConfig): FetchRequestConfig | Promise<FetchRequestConfig>
}

function joinUrl(base: string, path: string): string {
  if (!path) return base
  if (base.endsWith('/') && path.startsWith('/')) return base + path.slice(1)
  if (!base.endsWith('/') && !path.startsWith('/')) return base + '/' + path
  return base + path
}

function appendSearchParam(url: URL, name: string, value: QueryParamValue) {
  if (value === undefined || value === null) return

  if (Array.isArray(value)) {
    for (const item of value) {
      if (item === undefined || item === null) continue
      url.searchParams.append(name, String(item))
    }
    return
  }

  url.searchParams.append(name, String(value))
}

export function buildCatalogRequestUrl(
  catalogUrl: string,
  path: string,
  params?: Record<string, QueryParamValue>
) {
  const url = new URL(joinUrl(catalogUrl, path))
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      appendSearchParam(url, k, v)
    }
  }
  return url
}

export type CatalogAuthType = CatalogAuth

export interface RestCatalogClientOptions {
  catalogUrl: string
  auth: CatalogAuthType
  timeoutMs?: number
}

export interface CreateNamespaceRequest {
  namespace: string[]
  warehouse: string
  properties?: Record<string, string | number>
}

export interface CreateNamespaceResponse {
  namespace: string[]
  properties?: Record<string, string | number>
}

export interface DeleteNamespaceRequest {
  namespace: string
  warehouse: string
}

// TypeScript definitions for the inlined CreateTableRequest schema:

/** Represents a request to create a new Iceberg table */
export interface CreateTableRequest {
  namespace: string

  warehouse: string

  /** The table identifier (just the final name segment) */
  name: string

  /** Optional URI where table metadata/data will live */
  location?: string | null

  /** The table’s schema definition */
  schema: Schema

  /** The partitioning spec for the table */
  spec: PartitionSpec

  /** Arbitrary key/value properties to set on the table */
  properties?: Record<string, string>

  /**
   * If true, initialize metadata for a create transaction
   * instead of committing immediately
   */
  'stage-create'?: boolean

  /** The initial sort order for writes (optional) */
  'write-order'?: SortOrder | null
}

/** The Iceberg Schema object: always a struct of fields */
export interface Schema {
  type: 'struct'
  fields: Field[]

  /** schema identifier assigned by the catalog */
  /** Schema identifier assigned by the catalog */
  'schema-id'?: number

  /** list of identifier (primary key) field IDs */
  'identifier-field-ids'?: number[]
}

/** A single field in a Schema */
export interface Field {
  id: number
  name: string
  /** Field type: primitive or nested container */
  type: FieldType
  required: boolean
  doc?: string
}

/** Union of all possible Iceberg field types */
export type FieldType = PrimitiveType | StructType | ListType | MapType

/** Any primitive Iceberg type name */
export type PrimitiveType =
  | 'boolean'
  | 'integer'
  | 'long'
  | 'float'
  | 'double'
  | 'date'
  | 'time'
  | 'timestamp'
  | 'timestamptz'
  | 'string'
  | 'uuid'
  | 'fixed'
  | 'binary'

/** Nested struct type */
export interface StructType {
  type: 'struct'
  fields: Field[]
}

/** List type */
export interface ListType {
  type: 'list'
  /** Unique element field ID */
  'element-id': number
  /** Element’s data type (may recurse) */
  element: FieldType
  'element-required': boolean
}

/** Map type */
export interface MapType {
  type: 'map'
  'key-id': number
  key: FieldType
  'value-id': number
  value: FieldType
  'value-required': boolean
}

/** The Iceberg PartitionSpec object */
export interface PartitionSpec {
  /** spec identifier assigned by the catalog */
  'spec-id'?: number
  fields: PartitionField[]
}

/** A single partition field definition */
export interface PartitionField {
  /** field ID (catalog‐assigned) */
  'field-id'?: number
  'source-id': number
  name: string
  transform: string
}

/** The Iceberg SortOrder object */
export interface SortOrder {
  /** order identifier assigned by the catalog */
  'order-id'?: number
  fields: SortField[]
}

/** A single sort-field entry */
export interface SortField {
  'source-id': number
  transform: string
  direction: 'asc' | 'desc'
  'null-order': 'nulls-first' | 'nulls-last'
}

export interface ListTableRequest {
  namespace: string
  warehouse: string
  pageSize?: number
  pageToken?: string
}

export interface ListTableResponse {
  'next-page-token'?: string | undefined
  identifiers: {
    namespace: string[]
    name: string
  }[]
}

export interface LoadTableRequest {
  namespace: string
  table: string
  warehouse: string
  snapshots?: string
}

/**
 * Request to commit updates to multiple Iceberg tables in an atomic operation.
 */
export interface CommitTableRequest extends TableChange {
  /** List of changes to apply, one entry per table */
  namespace: string
  warehouse: string
  table: string
}

/** Changes for a single table */
export interface TableChange {
  /**
   * Assertions to validate before applying updates.
   * All must pass or the whole commit is rejected.
   */
  requirements?: Requirement[]

  /** Metadata updates to apply to the table */
  updates: TableUpdate[]
}

/** A pre-commit assertion requirement */
export interface Requirement {
  /** Name of the requirement (e.g. "assert-ref-snapshot-id") */
  name?: string

  /** Arguments for the requirement; arbitrary key/value pairs */
  args?: Record<string, unknown>
}

/** A single metadata update operation */
export interface TableUpdate {
  /** Name of the update operation (e.g. "add-column") */
  name?: string

  /** Arguments for the update; arbitrary key/value pairs */
  args?: Record<string, unknown>
}

export interface TableMetadata {
  /** The version of the table format (1 or 2). */
  'format-version': number
  /** A UUID that uniquely identifies the table. */
  'table-uuid': string
  /** The base location of the table. */
  location?: string
  /** When the metadata was last updated, in milliseconds since the epoch. */
  'last-updated-ms'?: number
  /** Arbitrary key/value table properties. */
  properties?: Record<string, string>
  /** Schema history for the table. */
  schemas?: Schema[]
  /** The ID of the current schema in the `schemas` array. */
  'current-schema-id'?: number

  'current-snapshot-id'?: number

  /** The last column ID assigned (for tracking new columns). */
  'last-column-id'?: number
  /** All known partition specs for the table. */
  'partition-specs'?: PartitionSpec[]
  /** The ID of the default partition spec in the `partition-specs` array. */
  'default-spec-id'?: number

  'metadata-log'?: MetadataLog[]
}

interface MetadataLog {
  'metadata-file': string
  'timestamp-ms': number
}

/**
 * Result returned when creating or loading a table.
 */
export interface LoadTableResult {
  /** The location of the table metadata file. */
  'metadata-location': string
  /** The deserialized table metadata. */
  metadata: TableMetadata
}

export interface NamespaceExistsRequest {
  namespace: string
  warehouse: string
}

export interface TableExistsRequest {
  namespace: string
  warehouse: string
  table: string
}

export interface LoadNamespaceMetadataRequest {
  namespace: string
  warehouse: string
}

export interface LoadNamespaceMetadataResponse {
  namespace: string[]
  properties?: Record<string, string | number>
}

export interface DropTableRequest {
  namespace: string
  warehouse: string
  table: string
  purgeRequested?: boolean
  tenantId?: string
}

/**
 * The response returned by the CreateTable endpoint.
 */
export type CreateTableResponse = LoadTableResult

export class RestCatalogClient {
  catalogUrl: string
  auth: CatalogAuthType
  timeoutMs: number

  constructor(options: RestCatalogClientOptions) {
    this.catalogUrl = options.catalogUrl
    this.auth = options.auth
    this.timeoutMs = options.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS
  }

  private async request<T>(input: FetchRequestInput): Promise<T> {
    const url = buildCatalogRequestUrl(this.catalogUrl, input.url, input.params)
    const headers = new Headers(input.headers || {})
    const body = input.data !== undefined ? JSONBigint.stringify(input.data) : undefined
    if (body !== undefined) {
      headers.set('Content-Type', 'application/json')
    }

    const authorized = await this.auth.authorize({
      method: (input.method || 'GET').toUpperCase(),
      url: url.toString(),
      headers,
      body,
    })

    try {
      const response = await fetch(authorized.url, {
        method: authorized.method,
        headers: authorized.headers,
        body: authorized.body,
        signal: AbortSignal.timeout(this.timeoutMs),
      })

      if (!response.ok) {
        const text = await response.text()
        let jsonResponse: unknown
        try {
          jsonResponse = JSONBigint.parse(text)
        } catch {
          jsonResponse = text
        }
        throw this.parseIcebergError(response.status, jsonResponse, input.notFoundResource)
      }

      const resText = await response.text()
      if (!resText) return undefined as unknown as T
      return JSONBigint.parse(resText) as T
    } catch (error) {
      if (error instanceof IcebergError) {
        throw error
      }
      throw ERRORS.InternalError(error as Error, 'Network error or Iceberg request failed')
    }
  }

  /**
   * Parse HTTP response status and body to create appropriate Iceberg error
   * Handles all HTTP error codes from the Iceberg REST specification
   */
  private parseIcebergError(
    status: number,
    data: unknown,
    notFoundResource?: NotFoundResource
  ): IcebergError {
    // Try to extract error details from response body
    if (data && typeof data === 'object') {
      try {
        // Map error types to specific error creators
        return IcebergError.fromResponse(data)
      } catch {
        // Fall through to status code handling
      }
    }

    // Handle specific status codes as per Iceberg spec
    return this.createErrorByStatusCode(status, notFoundResource)
  }

  /**
   * Create appropriate error based on HTTP status code
   */
  private createErrorByStatusCode(
    status: number,
    notFoundResource?: NotFoundResource
  ): IcebergError {
    switch (status) {
      case IcebergHttpStatusCode.BadRequest:
        return createBadRequestError('Bad request')
      case IcebergHttpStatusCode.Unauthorized:
        return createUnauthorizedError('Unauthorized')
      case IcebergHttpStatusCode.Forbidden:
        return createForbiddenError('Forbidden')
      case IcebergHttpStatusCode.NotFound:
        if (notFoundResource === 'table') {
          return createNoSuchTableError('Not found')
        }
        return createNoSuchNamespaceError('Not found')
      case IcebergHttpStatusCode.NotAcceptable:
        return createUnsupportedOperationError('Unsupported operation')
      case IcebergHttpStatusCode.Conflict:
        return createAlreadyExistsError('Conflict')
      case IcebergHttpStatusCode.UnprocessableEntity:
        return createUnprocessableEntityError('Unprocessable entity')
      case IcebergHttpStatusCode.AuthenticationTimeout:
        return createAuthenticationTimeoutError('Authentication timeout')
      case IcebergHttpStatusCode.ServiceUnavailable:
        return createSlowDownError('Service unavailable')
      default:
        if (status >= 500) {
          return createInternalServerError('Internal server error')
        }
        return createInternalServerError(`HTTP ${status}`)
    }
  }

  /**
   * Retrieves catalog configuration settings
   *
   * @see https://iceberg.apache.org/spec/api/#get-v1config
   * @param params Request parameters including warehouse identifier
   * @returns The catalog configuration response
   */
  async getConfig(params: GetConfigRequest) {
    const data = await this.request<GetConfigResponse>({
      url: '/config',
      method: 'GET',
      params: { warehouse: params.warehouse },
    })

    const overrides: NonNullable<GetConfigResponse['overrides']> = {
      prefix: params.warehouse,
    }

    return {
      defaults: {
        ...data.defaults,
        prefix: params.warehouse,
      },
      overrides,
    }
  }

  /**
   * Lists all namespaces in the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#list-namespaces
   * @param params Request parameters for listing namespaces
   * @returns List of namespace identifiers
   */
  async listNamespaces(params: ListNamespacesRequest) {
    const warehouse = this.getEncodedWarehouse(params.warehouse)
    return this.request<ListNamespacesResponse>({
      url: `${warehouse}/namespaces`,
      method: 'GET',
      params: { ...params },
    })
  }

  /**
   * Creates a new namespace in the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#create-namespace
   * @param params Request parameters for namespace creation
   * @returns The created namespace response
   */
  async createNamespace(params: CreateNamespaceRequest) {
    const warehouse = this.getEncodedWarehouse(params.warehouse)
    return this.request<CreateNamespaceResponse>({
      url: `${warehouse}/namespaces`,
      method: 'POST',
      data: {
        namespace: params.namespace,
        properties: params.properties || {},
      },
    })
  }

  /**
   * Loads metadata for a specific namespace
   *
   * @see https://iceberg.apache.org/spec/api/#get-namespace-properties
   * @param params Request parameters including the namespace name
   * @returns The namespace metadata
   */
  async loadNamespaceMetadata(params: LoadNamespaceMetadataRequest) {
    const warehouse = this.getEncodedWarehouse(params.warehouse)
    return this.request<LoadNamespaceMetadataResponse>({
      url: `${warehouse}/namespaces/${encodeURIComponent(params.namespace)}`,
      method: 'GET',
      notFoundResource: 'namespace',
    })
  }

  getEncodedWarehouse(warehouse: string) {
    return '/' + encodeURIComponent(warehouse)
  }

  /**
   * Deletes a namespace from the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#drop-namespace
   * @param params Request parameters for namespace deletion
   * @returns Void response after successful deletion
   */
  async dropNamespace(params: DeleteNamespaceRequest) {
    const warehouse = this.getEncodedWarehouse(params.warehouse)
    return this.request<void>({
      url: `${warehouse}/namespaces/${encodeURIComponent(params.namespace)}`,
      method: 'DELETE',
      notFoundResource: 'namespace',
    })
  }

  /**
   * Lists tables within a specified namespace
   *
   * @see https://iceberg.apache.org/spec/api/#list-tables
   * @param params Request parameters including namespace and pagination options
   * @returns List of table identifiers
   */
  async listTables({ namespace, ...rest }: ListTableRequest) {
    const warehouse = this.getEncodedWarehouse(rest.warehouse)
    return this.request<ListTableResponse>({
      url: `${warehouse}/namespaces/${encodeURIComponent(namespace)}/tables`,
      method: 'GET',
      params: rest,
      notFoundResource: 'namespace',
    })
  }

  /**
   * Creates a new table in the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#create-table
   * @param params Request parameters for table creation including schema and partition spec
   * @returns The created table metadata
   */
  async createTable({ namespace, ...rest }: CreateTableRequest) {
    const warehouse = this.getEncodedWarehouse(rest.warehouse)
    return this.request<CreateTableResponse>({
      url: `${warehouse}/namespaces/${encodeURIComponent(namespace)}/tables`,
      method: 'POST',
      data: rest,
      notFoundResource: 'namespace',
    })
  }

  /**
   * Loads metadata for a specific table
   *
   * @see https://iceberg.apache.org/spec/api/#load-table
   * @param params Request parameters identifying the table to load
   * @returns The table metadata and location
   */
  async loadTable(params: LoadTableRequest) {
    const warehouse = this.getEncodedWarehouse(params.warehouse)
    return this.request<LoadTableResult>({
      url: `${warehouse}/namespaces/${encodeURIComponent(params.namespace)}/tables/${encodeURIComponent(params.table)}`,
      method: 'GET',
      params: { snapshots: params.snapshots },
      notFoundResource: 'table',
    })
  }

  /**
   * Updates an existing table's metadata
   *
   * @see https://iceberg.apache.org/spec/api/#commit-table-changes
   * @param params Request parameters with table changes to apply
   * @returns The updated table metadata
   */
  async updateTable(params: CommitTableRequest) {
    const warehouse = this.getEncodedWarehouse(params.warehouse)
    return this.request<LoadTableResult>({
      url: `${warehouse}/namespaces/${encodeURIComponent(params.namespace)}/tables/${encodeURIComponent(params.table)}`,
      method: 'POST',
      data: params,
      notFoundResource: 'table',
    })
  }

  /**
   * Deletes a table from the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#drop-table
   * @param params Request parameters identifying the table to drop
   * @returns Void response after successful deletion
   */
  async dropTable(params: DropTableRequest) {
    const warehouse = this.getEncodedWarehouse(params.warehouse)
    const query: Record<string, string> = {}

    if (params.purgeRequested) {
      query.purgeRequested = 'true'
    }

    return this.request<void>({
      url: `${warehouse}/namespaces/${encodeURIComponent(params.namespace)}/tables/${encodeURIComponent(params.table)}`,
      method: 'DELETE',
      params: query,
      notFoundResource: 'table',
    })
  }

  /**
   * Renames a table in the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#rename-table
   * @param params Request parameters for table rename operation
   * @returns The updated table metadata
   */
  renameTable() {}

  /**
   * Checks if a specific table exists in the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#check-table-exists
   * @param params Request parameters identifying the table to check
   * @returns Boolean indicating if the table exists
   */
  async tableExists(params: TableExistsRequest) {
    const warehouse = this.getEncodedWarehouse(params.warehouse)
    return this.request<void>({
      url: `${warehouse}/namespaces/${encodeURIComponent(params.namespace)}/tables/${encodeURIComponent(params.table)}`,
      method: 'HEAD',
      notFoundResource: 'table',
    })
  }

  /**
   * Checks if a specific namespace exists in the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#check-namespace-exists
   * @param params Request parameters identifying the namespace to check
   * @returns Boolean indicating if the namespace exists
   */
  async namespaceExists(params: NamespaceExistsRequest) {
    const warehouse = this.getEncodedWarehouse(params.warehouse)
    return this.request<void>({
      url: `${warehouse}/namespaces/${encodeURIComponent(params.namespace)}`,
      method: 'HEAD',
      notFoundResource: 'namespace',
    })
  }
}

/**
 * SignV4Auth class for AWS Signature Version 4 authentication
 * This class implements the CatalogAuth interface
 * to sign requests to the S3Tables service.
 */
export class SignV4Auth {
  constructor(
    private readonly opts: {
      region: string
      credentials?: SignRequestOptions['credentials']
    }
  ) {}

  async authorize(req: FetchRequestConfig) {
    const signedReq = await signRequest(
      req.url,
      {
        method: req.method,
        headers: req.headers,
        body: req.body,
      },
      {
        service: 's3tables',
        region: this.opts.region,
        credentials: this.opts.credentials,
      }
    )

    signedReq.headers.forEach((headerValue, headerName) => {
      req.headers.set(headerName, headerValue)
    })

    return req
  }
}

/**
 * BearerTokenAuth class for Bearer token authentication
 * This class implements the CatalogAuth interface
 * to add a Bearer token to the request headers.
 */
export class BearerTokenAuth {
  constructor(private readonly opts: { token: string }) {}

  async authorize(req: FetchRequestConfig) {
    req.headers.set('Authorization', `Bearer ${this.opts.token}`)
    return req
  }
}
