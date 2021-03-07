export type Bucket = {
  id: string
  name: string
  owner: string
  createdAt: string
  updatedAt: string
}

export type Obj = {
  id: string
  bucketId: string
  name: string
  owner: string
  createdAt: string
  updatedAt: string
  lastAccessedAt: string
  metadata?: Record<string, unknown>
  buckets?: Bucket
}

export type signedToken = {
  url: string
}
