// eslint-disable-next-line @typescript-eslint/no-var-requires
const Sequencer = require('@jest/test-sequencer').default

// https://stackoverflow.com/a/68009048

const isRLSTest = (test) => {
  return test.path.includes('rls')
}

const isTusTest = (test) => {
  return test.path.includes('tus')
}

class CustomSequencer extends Sequencer {
  sort(tests) {
    const copyTests = Array.from(tests)
    const normalTests = copyTests.filter((t) => !isRLSTest(t) && !isTusTest(t))
    const tusTests = copyTests.filter((t) => isTusTest(t))
    const rlsTests = copyTests.filter((t) => isRLSTest(t))
    return super
      .sort(normalTests)
      .concat(tusTests)
      .concat(rlsTests.sort((a, b) => (a.path > b.path ? 1 : -1)))
  }
}

module.exports = CustomSequencer
