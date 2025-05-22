// eslint-disable-next-line @typescript-eslint/no-var-requires
const Sequencer = require('@jest/test-sequencer').default

// order to sort tests based on name matching
const testOrder = [
  'jwt.test.ts', // needs to run before it is imported by other libraries for proper coverage
  '*', // all other tests not matched
  'tus.test.ts',
  's3-protocol.test.ts',
  'rls.test.ts',
]

class CustomSequencer extends Sequencer {
  sort(tests) {
    const testBuckets = {}
    testOrder.forEach(k => {
      testBuckets[k] = []
    })

    Array.from(tests).forEach(t => {
      const fileName = t.path.split('/').pop()
      const bucket = testBuckets[fileName] || testBuckets['*']
      bucket.push(t)
    })
    return testOrder.flatMap(k => super.sort(testBuckets[k]))
  }
}

module.exports = CustomSequencer
