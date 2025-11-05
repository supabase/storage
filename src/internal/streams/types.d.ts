declare module 'stream' {
  export function compose<A extends Stream, B extends Stream>(s1: A, s2: B): B & A
}
