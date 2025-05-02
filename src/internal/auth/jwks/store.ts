export interface JWKStoreItem {
  id: string
  content: string
  kind: string
}

export interface PaginatedTenantItem {
  id: string
  cursor_id: number
}

export interface JWKSManagerStore<TRX> {
  /**
   * Adds a jwk to the database
   * @param tenant_id owning tenant
   * @param content serialized and encrypted jwk content
   * @param kind the kind identifier for this jwk
   * @param idempotent inserts idempotent. Depends on a unique index for the provided kind
   * @param trx optional transaction to use for this query
   */
  insert(
    tenantId: string,
    encryptedJwk: string,
    kind: string,
    idempotent?: boolean,
    trx?: TRX
  ): Promise<string>

  /**
   * Sets the active value for a jwk by id
   * @param tenantId
   * @param id
   * @param newState
   */
  toggleActive(tenantId: string, id: string, newState: boolean): Promise<boolean>

  /**
   * Lists all active jwks for the specified tenant
   * @param tenantId
   */
  listActive(tenantId: string): Promise<JWKStoreItem[]>

  /**
   * Lists tenants that do not have a jwk of the specified kind
   * @param kind
   * @param batchSize
   * @param lastCursor
   */
  listTenantsWithoutKindPaginated(
    kind: string,
    batchSize: number,
    lastCursor?: number
  ): Promise<PaginatedTenantItem[]>
}
