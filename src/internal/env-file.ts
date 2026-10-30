import { loadEnvFile } from 'node:process'

export function loadOptionalEnvFile(path?: string) {
  try {
    loadEnvFile(path)
  } catch {
    // Environment files are optional; configuration may come entirely from the process.
  }
}
