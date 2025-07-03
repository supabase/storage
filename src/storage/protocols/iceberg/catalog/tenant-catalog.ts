import {
  CatalogAuthType,
  CommitTableRequest,
  CreateNamespaceRequest,
  CreateTableRequest,
  DeleteNamespaceRequest,
  DropTableRequest,
  ListNamespacesRequest,
  ListTableRequest,
  LoadNamespaceMetadataRequest,
  LoadTableRequest,
  NamespaceExistsRequest,
  RestCatalogClient,
  TableExistsRequest,
} from './rest-catalog-client'
import { Metastore } from '../knex'
import { ERRORS } from '@internal/errors'

/**
 * Configuration options for the tenant-aware REST catalog client
 */
export interface RestCatalogTenantOptions {
  accessKeyId?: string
  secretAccessKey?: string
  tenantId: string
  restCatalogUrl: string
  warehouse: string
  metastore: Metastore
  auth: CatalogAuthType
  limits: {
    maxCatalogsCount: number
    maxNamespaceCount: number
    maxTableCount: number
  }
}
/**
 * Parameters for finding a table by its ID
 */
interface FindTableByIdParams {
  id: string
  tenantId: string
  namespaceId: string
}

/**
 * A tenant-aware REST catalog implementation for Iceberg tables
 *
 * This class extends the base REST catalog proxy and adds tenant isolation features,
 * mapping tenant-specific names to internal representations and managing permissions.
 */

export class TenantAwareRestCatalog extends RestCatalogClient {
  tenantId: string

  /**
   * Creates a new tenant-aware REST catalog client
   * @param options Configuration options for the catalog
   */
  constructor(private readonly options: RestCatalogTenantOptions) {
    super({
      connectionString: options.restCatalogUrl,
      warehouse: options.warehouse,
      auth: options.auth,
    })

    this.tenantId = options.tenantId
  }

  findCatalogById(params: { tenantId: string; id: string }) {
    // Find the catalog by bucket ID and tenant ID in the metastore
    return this.options.metastore.findCatalogById({
      id: params.id,
      tenantId: params.tenantId,
    })
  }

  findTableByLocation(params: { location: string; tenantId: string }) {
    // Find the table by its location in the metastore
    return this.options.metastore.findTableByLocation({
      location: params.location,
      tenantId: params.tenantId,
    })
  }

  async registerCatalog(params: { bucketId: string; tenantId: string }) {
    // Register the catalog with the Iceberg REST API
    return this.options.metastore.transaction(async (store) => {
      const catalogCount = await store.countCatalogs({
        tenantId: params.tenantId,
        limit: this.options.limits.maxCatalogsCount + 1,
      })

      if (catalogCount > this.options.limits.maxCatalogsCount) {
        throw ERRORS.IcebergMaximumResourceLimit(this.options.limits.maxCatalogsCount)
      }

      return store.assignCatalog({
        bucketId: params.bucketId,
        tenantId: params.tenantId,
      })
    })
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
    this.validateResourceName(rest.name)

    return this.options.metastore.transaction(
      async (store) => {
        const dbNamespace = await store.findNamespaceByName({
          name: namespace,
          tenantId: this.tenantId,
        })

        const tableCount = await store.countTables({
          tenantId: this.tenantId,
          limit: this.options.limits.maxTableCount,
          namespaceId: dbNamespace.id,
        })

        if (tableCount >= this.options.limits.maxTableCount) {
          throw ERRORS.IcebergMaximumResourceLimit(this.options.limits.maxTableCount)
        }

        const namespaceName = this.getTenantNamespaceName(dbNamespace.id)

        try {
          const table = await super.createTable({ namespace: namespaceName, ...rest })
          await store.createTable({
            name: rest.name,
            bucketId: rest.warehouse,
            namespaceId: dbNamespace.id,
            tenantId: this.options.tenantId,
            location: table['metadata'].location as string,
          })

          return table
        } catch (e) {
          throw e
        }
      },
      {
        isolationLevel: 'serializable',
      }
    )
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

    const tables = await this.options.metastore.listTables({
      tenantId: this.tenantId,
      pageSize: params.pageSize,
      namespaceId: dbNamespace.id,
    })

    const identifiers = tables.map((table) => {
      const namespace = params.namespace
      return {
        name: table.name,
        namespace: [namespace],
      }
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

    const namespaceName = this.getTenantNamespaceName(namespace.id)

    const [table, dbTable] = await Promise.all([
      super.loadTable({
        ...params,
        namespace: namespaceName,
        snapshots: 'all',
      }),
      this.options.metastore.findTableByName({
        tenantId: this.tenantId,
        name: params.table,
      }),
    ])

    return table
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
    return this.options.metastore.transaction(
      async (store) => {
        const namespace = await store.findNamespaceByName({
          tenantId: this.tenantId,
          name: params.namespace,
        })

        const namespaceName = this.getTenantNamespaceName(namespace.id)

        const table = await super.updateTable({
          ...params,
          namespace: namespaceName,
        })

        await store.findTableByName({
          tenantId: this.tenantId,
          name: params.table,
        })

        return table
      },
      {
        isolationLevel: 'serializable',
      }
    )
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

    const namespaceName = this.getTenantNamespaceName(namespace.id)

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

    const namespaceName = this.getTenantNamespaceName(namespace.id)

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
    this.validateResourceName(params.namespace[0])

    return this.options.metastore.transaction(
      async (store) => {
        const namespace = await store.assignNamespace({
          name: params.namespace[0],
          bucketId: params.warehouse,
          tenantId: this.options.tenantId,
        })

        const namespaceCount = await store.countNamespaces({
          tenantId: this.tenantId,
          limit: this.options.limits.maxNamespaceCount + 1,
        })

        if (namespaceCount > this.options.limits.maxNamespaceCount) {
          throw ERRORS.IcebergMaximumResourceLimit(this.options.limits.maxNamespaceCount)
        }

        const namespaceName = this.getTenantNamespaceName(namespace.id)

        const namespaceResp = await super.createNamespace({
          namespace: [namespaceName],
          properties: params.properties,
          warehouse: params.warehouse,
        })

        return { namespace: [namespace.name], properties: namespaceResp.properties || {} }
      },
      {
        isolationLevel: 'serializable',
      }
    )
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

    const namespaceName = this.getTenantNamespaceName(namespace.id)

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

  async dropTable(params: DropTableRequest) {
    const namespace = await this.findNamespaceByName({
      name: params.namespace,
      tenantId: this.tenantId,
    })

    const namespaceName = this.getTenantNamespaceName(namespace.id)

    return this.options.metastore.transaction(
      async (store) => {
        await store.dropTable({
          table: params.table,
          namespace: namespace.id,
          tenantId: this.tenantId,
        })

        return super.dropTable({
          ...params,
          namespace: namespaceName,
        })
      },
      {
        isolationLevel: 'serializable',
      }
    )
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

    const namespaceName = this.getTenantNamespaceName(namespace.id)

    return this.options.metastore.transaction(
      async (store) => {
        await store.dropNamespace({
          namespace: namespace.name,
          bucketId: params.warehouse,
          tenantId: this.tenantId,
        })

        await super.dropNamespace({
          ...params,
          namespace: namespaceName,
        })
      },
      {
        isolationLevel: 'serializable',
      }
    )
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
   * Converts a namespace to a tenant-specific format
   * @param namespace
   * @protected
   */
  protected getTenantNamespaceName(namespace: string) {
    // Convert the namespace to the tenant-specific format
    return `${this.tenantId}_${namespace.replaceAll('-', '_')}`
  }

  private validateResourceName(namespace: string) {
    if (!namespace || namespace.length < 1 || namespace.length > 255) {
      throw ERRORS.InvalidParameter('namespace', {
        message: 'Resource name must be between 1 and 255 characters long',
      })
    }

    if (!/^[a-z0-9][a-z0-9_]*[a-z0-9]$|^[a-z0-9]$/.test(namespace)) {
      throw ERRORS.InvalidParameter('namespace', {
        message:
          'Resource name must contain only lowercase letters, numbers, and underscores, and must begin and end with a letter or number',
      })
    }

    if (namespace.includes('-') || namespace.includes('.')) {
      throw ERRORS.InvalidParameter('namespace', {
        message: 'Resource name must not contain hyphens (-) or periods (.)',
      })
    }

    if (namespace.startsWith('aws')) {
      throw ERRORS.InvalidParameter('namespace', {
        message: 'Resource name must not start with the reserved prefix "aws"',
      })
    }

    if (namespace.endsWith('--iceberg')) {
      throw ERRORS.InvalidParameter('namespace', {
        message: 'Resource name must not end with the reserved suffix "--iceberg"',
      })
    }
  }
}
