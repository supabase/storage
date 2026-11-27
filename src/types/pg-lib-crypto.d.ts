declare module 'pg/lib/crypto/utils' {
  const utils: {
    deriveKey(password: string, salt: Uint8Array, iterations: number): Promise<ArrayBuffer>
    hmacSha256(key: ArrayBuffer | Uint8Array, msg: string): Promise<ArrayBuffer | Uint8Array>
    sha256(data: ArrayBuffer | Uint8Array): Promise<ArrayBuffer | Uint8Array>
  }
  export default utils
}

declare module 'pg/lib/crypto/sasl' {
  interface Session {
    mechanism: string
    clientNonce: string
    response: string
    message: string
  }
  const sasl: {
    continueSession(session: Session, password: string, serverData: string): Promise<void>
    finalizeSession(session: Session, serverData: string): void
  }
  export default sasl
}
