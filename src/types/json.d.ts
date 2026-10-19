// Node 24 provides JSON.rawJSON, which is not yet declared by TypeScript's library.
interface JSON {
  rawJSON(text: string): { readonly rawJSON: string }
}
