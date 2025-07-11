import axios, { AxiosError, AxiosInstance, InternalAxiosRequestConfig } from 'axios'
import { ErrorCode, ERRORS, StorageBackendError } from '@internal/errors'
import { signRequest } from 'aws-sigv4-sign'

export interface GetConfigRequest {
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
  bucketId: string
  pageToken?: string
  pageSize?: number
  parent?: string
}

export interface ListNamespacesResponse {
  namespaces: string[][]
  nextPageToken?: string
}

interface CatalogAuth {
  authorize(
    req: InternalAxiosRequestConfig<string>
  ): InternalAxiosRequestConfig<string> | Promise<InternalAxiosRequestConfig<string>>
}

export type CatalogAuthType = CatalogAuth

export interface RestCatalogClientOptions {
  connectionString: string
  warehouse: string
  auth: CatalogAuthType
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
  pageSize?: number
  pageToken?: string
}

export interface ListTableResponse {
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
}

export interface TableExistsRequest {
  namespace: string
  table: string
}

export interface LoadNamespaceMetadataRequest {
  namespace: string
}

export interface LoadNamespaceMetadataResponse {
  namespace: string[]
  properties?: Record<string, string | number>
}

export interface DropTableRequest {
  namespace: string
  table: string
  tenantId?: string
}

/**
 * The response returned by the CreateTable endpoint.
 */
export type CreateTableResponse = LoadTableResult

export class RestCatalogClient {
  httpClient: AxiosInstance
  warehouse: string
  auth: CatalogAuthType

  constructor(options: RestCatalogClientOptions) {
    this.httpClient = axios.create({
      baseURL: options.connectionString,
    })

    this.warehouse = options.warehouse
    this.auth = options.auth

    this.httpClient.interceptors.request.use((req) => {
      return this.auth.authorize(req)
    })

    this.httpClient.interceptors.response.use(
      // On 2xx responses, just pass through
      (response) => response,

      // On errors…
      (error) => {
        // If there's no response, it’s a network / CORS / timeout error
        if (!error.response) {
          throw ERRORS.InternalError(error, 'Network error')
        }

        if (error instanceof AxiosError) {
          console.log('Iceberg request failed:', {
            status: error.response.status,
            data: error.response.data,
            headers: error.response.headers,
            message: error.message,
          })
          // Throw your custom error
          throw new StorageBackendError({
            message: error.message,
            error: error.message,
            httpStatusCode: error.response.status,
            code: ErrorCode.IcebergError,
            originalError: error,
          })
        }

        throw ERRORS.InternalError(error, 'Iceberg request failed')
      }
    )
  }

  /**
   * Retrieves catalog configuration settings
   *
   * @see https://iceberg.apache.org/spec/api/#get-v1config
   * @param params Request parameters including warehouse identifier
   * @returns The catalog configuration response
   */
  async getConfig(params: GetConfigRequest) {
    return this.httpClient
      .get<GetConfigResponse>('/config', {
        params: {
          warehouse: this.warehouse,
        },
      })
      .then((response) => {
        const data = response.data

        const overrides: Record<string, any> = {
          prefix: params.warehouse,
        }

        return {
          defaults: {
            ...data.defaults,
            prefix: params.warehouse,
          },
          overrides,
        }
      })
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }

  /**
   * Lists all namespaces in the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#list-namespaces
   * @param params Request parameters for listing namespaces
   * @returns List of namespace identifiers
   */
  listNamespaces(params: ListNamespacesRequest) {
    const warehouse = this.getWarehouse()
    return this.httpClient
      .get<ListNamespacesResponse>(`${warehouse}/namespaces`, { params })
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }

  /**
   * Creates a new namespace in the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#create-namespace
   * @param params Request parameters for namespace creation
   * @returns The created namespace response
   */
  createNamespace(params: CreateNamespaceRequest) {
    const warehouse = this.getWarehouse()
    return this.httpClient
      .post<CreateNamespaceResponse>(`${warehouse}/namespaces`, {
        namespace: params.namespace,
        properties: params.properties || {},
      })
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }

  /**
   * Loads metadata for a specific namespace
   *
   * @see https://iceberg.apache.org/spec/api/#get-namespace-properties
   * @param params Request parameters including the namespace name
   * @returns The namespace metadata
   */
  loadNamespaceMetadata(params: LoadNamespaceMetadataRequest) {
    const warehouse = this.getWarehouse()
    return this.httpClient
      .get<LoadNamespaceMetadataResponse>(`${warehouse}/namespaces/${params.namespace}`)
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }

  getWarehouse() {
    if (!this.warehouse) {
      return ''
    }
    return '/' + encodeURIComponent(this.warehouse)
  }

  /**
   * Deletes a namespace from the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#drop-namespace
   * @param params Request parameters for namespace deletion
   * @returns Void response after successful deletion
   */
  dropNamespace(params: DeleteNamespaceRequest) {
    const warehouse = this.getWarehouse()
    return this.httpClient
      .delete<void>(`${warehouse}/namespaces/${params.namespace}`)
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }

  /**
   * Lists tables within a specified namespace
   *
   * @see https://iceberg.apache.org/spec/api/#list-tables
   * @param params Request parameters including namespace and pagination options
   * @returns List of table identifiers
   */
  listTables({ namespace, ...rest }: ListTableRequest) {
    const warehouse = this.getWarehouse()

    return this.httpClient
      .get<ListTableResponse>(`${warehouse}/namespaces/${namespace}/tables`, {
        params: rest,
      })
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }

  /**
   * Creates a new table in the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#create-table
   * @param params Request parameters for table creation including schema and partition spec
   * @returns The created table metadata
   */
  createTable({ namespace, ...rest }: CreateTableRequest) {
    const warehouse = this.getWarehouse()
    return this.httpClient
      .post<CreateTableResponse>(`${warehouse}/namespaces/${namespace}/tables`, rest)
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }

  /**
   * Loads metadata for a specific table
   *
   * @see https://iceberg.apache.org/spec/api/#load-table
   * @param params Request parameters identifying the table to load
   * @returns The table metadata and location
   */
  loadTable(params: LoadTableRequest) {
    const warehouse = this.getWarehouse()
    return this.httpClient
      .get<LoadTableResult>(`${warehouse}/namespaces/${params.namespace}/tables/${params.table}`, {
        params: {
          snapshots: params.snapshots,
        },
      })
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }

  /**
   * Updates an existing table's metadata
   *
   * @see https://iceberg.apache.org/spec/api/#commit-table-changes
   * @param params Request parameters with table changes to apply
   * @returns The updated table metadata
   */
  updateTable(params: CommitTableRequest) {
    const warehouse = this.getWarehouse()
    return this.httpClient
      .post<LoadTableResult>(
        `${warehouse}/namespaces/${params.namespace}/tables/${params.table}`,
        params
      )
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }

  /**
   * Deletes a table from the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#drop-table
   * @param params Request parameters identifying the table to drop
   * @returns Void response after successful deletion
   */
  dropTable(params: DropTableRequest) {
    const warehouse = this.getWarehouse()
    return this.httpClient
      .delete<void>(`${warehouse}/namespaces/${params.namespace}/${params.table}`, {
        params: { purgeRequested: 'true', purge: 'true' },
      })
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
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
  tableExists(params: TableExistsRequest) {
    const warehouse = this.getWarehouse()
    return this.httpClient
      .head<void>(`${warehouse}/namespaces/${params.namespace}/tables/${params.table}`)
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }

  /**
   * Checks if a specific namespace exists in the catalog
   *
   * @see https://iceberg.apache.org/spec/api/#check-namespace-exists
   * @param params Request parameters identifying the namespace to check
   * @returns Boolean indicating if the namespace exists
   */
  namespaceExists(params: NamespaceExistsRequest) {
    const warehouse = this.getWarehouse()
    return this.httpClient
      .head<void>(`${warehouse}/namespaces/${params.namespace}`)
      .then((response) => response.data)
      .catch((error) => {
        if (error instanceof AxiosError) {
          console.error('Error fetching configuration:', error.response?.data)
        }
        throw error
      })
  }
}

/**
 * SignV4Auth class for AWS Signature Version 4 authentication
 * This class implements the CatalogAuth interface
 * to sign requests to the S3Tables service.
 */
export class SignV4Auth {
  constructor(private readonly opts: { region: string }) {}

  async authorize(req: InternalAxiosRequestConfig<string>) {
    const queryParams = Object.keys(req.params || {}).reduce((acc, name) => {
      if (req.params[name]) {
        acc[name] = req.params[name]
      }

      return acc
    }, {} as Record<string, string>)

    const queryString = new URLSearchParams(queryParams).toString()

    const signedReq = await signRequest(
      ((req.baseURL || '') + req.url + (queryString ? `?${queryString}` : '')) as string,
      {
        method: req.method?.toUpperCase(),
        headers: req.headers,
        body: req.data ? JSON.stringify(req.data) : undefined,
      },
      {
        service: 's3tables',
        region: this.opts.region,
      }
    )

    signedReq.headers.forEach((headerValue, headerName) => {
      req.headers.set(headerName, headerValue as string, true)
    })

    return req
  }
}

/**
 * TokenAuth class for Bearer token authentication
 * This class implements the CatalogAuth interface
 * to add a Bearer token to the request headers.
 */
export class TokenAuth {
  constructor(private readonly opts: { token: string }) {}

  async authorize(req: InternalAxiosRequestConfig<string>) {
    req.headers.set('Authorization', `Bearer ${this.opts.token}`, true)

    return req
  }
}
