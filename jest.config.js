module.exports = {
  preset: 'ts-jest',
  // testSequencer: './jest.sequencer.js',
  transform: {
    '^.+\\.(t|j)sx?$': 'ts-jest',
  },
  testMatch: ['src/tests/**/*.steps.js'],
  setupFilesAfterEnv: ['<rootDir>/jest-setup.ts'],
  testEnvironment: 'node',
  testPathIgnorePatterns: ['node_modules', 'dist'],
  coverageProvider: 'v8',
}
