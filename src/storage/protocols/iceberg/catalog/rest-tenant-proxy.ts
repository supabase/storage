import { signRequest } from 'aws-sigv4-sign'
import { InternalAxiosRequestConfig } from 'axios'
import {
  CommitTableRequest,
  CreateNamespaceRequest,
  CreateTableRequest,
  DeleteNamespaceRequest,
  ListNamespacesRequest,
  ListTableRequest,
  LoadNamespaceMetadataRequest,
  LoadTableRequest,
  LoadTableResult,
  NamespaceExistsRequest,
  RestCatalogProxy,
  TableExistsRequest,
} from './rest-base-proxy'
import { Metastore } from '../knex'
import { ERRORS } from '@internal/errors'

/**
 * Configuration options for the tenant-aware REST catalog client
 */
export interface RestCatalogTenantOptions {
  accessKeyId?: string
  secretAccessKey?: string
  tenantId: string
  region: string
  restCatalogUrl: string
  warehouse: string
  metastore: Metastore
  signatureSecret: string
}
/**
 * Parameters for finding a table by its ID
 */
interface FindTableByIdParams {
  id: string
  tenantId: string
}

/**
 * A tenant-aware REST catalog implementation for Iceberg tables
 *
 * This class extends the base REST catalog proxy and adds tenant isolation features,
 * mapping tenant-specific names to internal representations and managing permissions.
 */

export class RestCatalogTenant extends RestCatalogProxy {
  tenantId: string
  signatureSecret: string

  /**
   * Creates a new tenant-aware REST catalog client
   * @param options Configuration options for the catalog
   */
  constructor(private readonly options: RestCatalogTenantOptions) {
    super({
      connectionString: options.restCatalogUrl,
      warehouse: options.warehouse,
    })

    this.tenantId = options.tenantId
    this.signatureSecret = options.signatureSecret
  }

  /**
   * Creates a new Iceberg table for the current tenant
   *
   * Maps the tenant's namespace to an internal representation and registers
   * the table in both the Iceberg catalog and the local metastore.
   *
   * @param param0 Table creation parameters
   * @returns The created table with modified location paths
   */
  async createTable({ namespace, ...rest }: CreateTableRequest) {
    const dbNamespace = await this.findNamespaceByName({
      name: namespace,
      tenantId: this.tenantId,
    })

    const namespaceName = `${this.tenantId}_${dbNamespace.id.replaceAll('-', '_')}`

    const table = await super.createTable({ namespace: namespaceName, ...rest })

    const dbTable = await this.options.metastore.createTable({
      name: rest.name,
      namespaceId: dbNamespace.id,
      tenantId: this.options.tenantId,
      location: table['metadata'].location as string,
    })

    return this.modifyTableLocation(dbTable.id, table)
  }

  /**
   * Lists tables in the specified namespace for the current tenant
   *
   * Maps the tenant's namespace to an internal representation and returns
   * the tables with tenant-friendly identifiers.
   *
   * @param params List tables request parameters
   * @returns List of table identifiers in the namespace
   */
  async listTables(params: ListTableRequest) {
    const dbNamespace = await this.findNamespaceByName({
      name: params.namespace,
      tenantId: this.tenantId,
    })

    const namespaceName = `${this.tenantId}_${dbNamespace.id.replaceAll('-', '_')}`

    const tables = await super.listTables({
      ...params,
      namespace: namespaceName,
    })

    const identifiers = tables.identifiers.map((table) => {
      const namespace = params.namespace
      return { name: table.name, namespace: [namespace] }
    })

    return {
      identifiers,
    }
  }

  /**
   * Loads a table by name from the specified namespace for the current tenant
   *
   * Maps the tenant's namespace to an internal representation and returns
   * the table with tenant-friendly paths.
   *
   * @param params Load table request parameters
   * @returns The loaded table with modified location paths
   */
  async loadTable(params: LoadTableRequest) {
    const namespace = await this.options.metastore.findNamespaceByName({
      tenantId: this.tenantId,
      name: params.namespace,
    })

    const namespaceName = `${this.tenantId}_${namespace.id.replaceAll('-', '_')}`

    const table = await super.loadTable({
      ...params,
      namespace: namespaceName,
    })

    const dbTable = await this.options.metastore.findTableByName({
      tenantId: this.tenantId,
      name: params.table,
    })

    return this.modifyTableLocation(dbTable.id, table)
  }

  /**
   * Updates an existing Iceberg table for the current tenant
   *
   * Maps the tenant's namespace to an internal representation and updates
   * the table in the Iceberg catalog.
   *
   * @param params Update table request parameters
   * @returns The updated table with modified location paths
   */
  async updateTable(params: CommitTableRequest) {
    const namespace = await this.options.metastore.findNamespaceByName({
      tenantId: this.tenantId,
      name: params.namespace,
    })

    const namespaceName = `${this.tenantId}_${namespace.id.replaceAll('-', '_')}`

    const table = await super.updateTable({
      ...params,
      namespace: namespaceName,
    })

    const dbTable = await this.options.metastore.findTableByName({
      tenantId: this.tenantId,
      name: params.table,
    })

    return this.modifyTableLocation(dbTable.id, table)
  }

  /**
   * Checks if a table exists in the specified namespace for the current tenant
   *
   * Maps the tenant's namespace to an internal representation before checking.
   *
   * @param params Table exists request parameters
   * @returns Boolean indicating if the table exists
   */
  async tableExists(params: TableExistsRequest) {
    const namespace = await this.options.metastore.findNamespaceByName({
      tenantId: this.tenantId,
      name: params.namespace,
    })

    const namespaceName = `${this.tenantId}_${namespace.id.replaceAll('-', '_')}`

    return super.tableExists({
      ...params,
      namespace: namespaceName,
    })
  }

  /**
   * Checks if a namespace exists for the current tenant
   *
   * Maps the tenant's namespace to an internal representation before checking.
   *
   * @param params Namespace exists request parameters
   * @returns Boolean indicating if the namespace exists
   */
  async namespaceExists(params: NamespaceExistsRequest) {
    const namespace = await this.options.metastore.findNamespaceByName({
      tenantId: this.tenantId,
      name: params.namespace,
    })

    const namespaceName = `${this.tenantId}_${namespace.id.replaceAll('-', '_')}`

    return super.namespaceExists({
      ...params,
      namespace: namespaceName,
    })
  }

  /**
   * Creates a new namespace for the current tenant
   *
   * Assigns a unique internal ID for the namespace and creates both
   * a metastore entry and the physical namespace in the catalog.
   *
   * @param params Create namespace request parameters
   * @returns The created namespace identifier
   */
  async createNamespace(params: CreateNamespaceRequest) {
    const namespace = await this.options.metastore.assignNamespace({
      name: params.namespace[0],
      bucketId: params.warehouse,
      tenantId: this.options.tenantId,
    })

    const namespaceName = `${this.tenantId}_${namespace.id.replaceAll('-', '_')}`

    await super.createNamespace({
      namespace: [namespaceName],
      properties: params.properties,
      warehouse: params.warehouse,
    })

    return { namespace: [namespace.name] }
  }

  /**
   * Loads metadata for a namespace for the current tenant
   *
   * Maps the tenant's namespace to an internal representation before loading.
   *
   * @param params Load namespace metadata request parameters
   * @returns The namespace metadata
   */
  async loadNamespaceMetadata(params: LoadNamespaceMetadataRequest) {
    const namespace = await this.findNamespaceByName({
      name: params.namespace,
      tenantId: this.tenantId,
    })

    const namespaceName = `${this.tenantId}_${namespace.id.replaceAll('-', '_')}`

    return super.loadNamespaceMetadata({
      ...params,
      namespace: namespaceName,
    })
  }

  /**
   * Lists all namespaces for the current tenant
   *
   * @param params List namespaces request parameters
   * @returns List of namespace identifiers
   */
  async listNamespaces(params: ListNamespacesRequest) {
    const namespaces = await this.options.metastore.listNamespaces({
      bucketId: params.bucketId,
      tenantId: this.tenantId,
    })

    return { namespaces: namespaces.map((n) => [n.name]) }
  }

  /**
   * Deletes a namespace for the current tenant
   *
   * Maps the tenant's namespace to an internal representation before deleting.
   *
   * @param params Delete namespace request parameters
   */
  async dropNamespace(params: DeleteNamespaceRequest) {
    const namespace = await this.findNamespaceByName({
      name: params.namespace,
      tenantId: this.tenantId,
    })

    const namespaceName = `${this.tenantId}_${namespace.id.replaceAll('-', '_')}`

    await this.options.metastore.dropNamespace({
      name: namespaceName,
      bucketId: params.warehouse,
      tenantId: this.tenantId,
    })

    await super.dropNamespace(params)
  }

  /**
   * Authorizes HTTP requests using AWS signature v4
   *
   * Adds authentication headers to the request before sending it to the catalog service.
   *
   * @param req The Axios request configuration
   * @returns The modified request with authorization headers
   */
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
        region: this.options.region,
      }
    )

    signedReq.headers.forEach((headerValue, headerName) => {
      req.headers.set(headerName, headerValue as string, true)
    })

    return req
  }

  /**
   * Finds a table by its ID for a specific tenant
   *
   * @param params Find table parameters including tenant ID and table ID
   * @returns The table metadata
   */
  async findTableById(params: FindTableByIdParams) {
    return this.options.metastore.findTableById(params)
  }

  /**
   * Finds a namespace by name for a specific tenant
   *
   * @param params Find namespace parameters including tenant ID and namespace name
   * @returns The namespace metadata
   */
  findNamespaceByName(params: { name: string; tenantId: string }) {
    return this.options.metastore.findNamespaceByName(params)
  }

  /**
   * Transforms internal S3 paths to tenant-friendly paths
   *
   * Rewrites location fields in the table metadata to use the tenant-specific
   * bucket name pattern (tableId--iceberg).
   *
   * @param userBucket The table ID to use as the bucket prefix
   * @param table The original table metadata from the catalog
   * @returns The modified table metadata with tenant-friendly paths
   */
  protected modifyTableLocation(userBucket: string, table: LoadTableResult) {
    const internalBucketLocation = table.metadata.location?.replace('s3://', '')

    if (!internalBucketLocation) {
      throw ERRORS.InvalidParameter('location')
    }

    const storageLocation = `s3://${userBucket}--iceberg`

    const metadataLocation = table['metadata-location'].replace(
      `s3://${internalBucketLocation}`,
      storageLocation
    )

    return {
      ...table,
      'metadata-location': metadataLocation,
      metadata: {
        ...table.metadata,
        location: storageLocation,
      },
    }
  }
}
