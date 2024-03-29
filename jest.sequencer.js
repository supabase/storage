// eslint-disable-next-line @typescript-eslint/no-var-requires
const Sequencer = require('@jest/test-sequencer').default

// https://stackoverflow.com/a/68009048

const isRLSTest = (test) => {
  return test.path.includes('rls')
}

const isTusTest = (test) => {
  return test.path.includes('tus')
}

const isS3Test = (test) => {
  return test.path.includes('s3')
}

class CustomSequencer extends Sequencer {
  sort(tests) {
    const copyTests = Array.from(tests)
    const normalTests = copyTests.filter((t) => !isRLSTest(t) && !isTusTest(t) && !isS3Test(t))
    const tusTests = copyTests.filter((t) => isTusTest(t))
    const s3Tests = copyTests.filter((t) => isS3Test(t))
    const rlsTests = copyTests.filter((t) => isRLSTest(t))
    return super
      .sort(normalTests)
      .concat(tusTests)
      .concat(s3Tests)
      .concat(rlsTests.sort((a, b) => (a.path > b.path ? 1 : -1)))
  }
}

module.exports = CustomSequencer
