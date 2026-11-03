import path from 'node:path'
import { BaseSequencer, type TestSpecification } from 'vitest/node'

// Glob discovery order isn't alphabetical, so a filename alone can't guarantee a spec runs last
// usage-checksum must run last to be able to fail based on usage logs emitted by earlier tests
const LAST_FILE = 'usage-checksum.test.ts'

export default class AcceptanceSequencer extends BaseSequencer {
  async sort(files: TestSpecification[]): Promise<TestSpecification[]> {
    const sorted = await super.sort(files)
    const lastIndex = sorted.findIndex((file) => path.basename(file.moduleId) === LAST_FILE)

    if (lastIndex !== -1) {
      sorted.push(...sorted.splice(lastIndex, 1))
    }

    return sorted
  }
}
