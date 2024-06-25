module.exports = {
  preset: 'ts-jest',
  testSequencer: './jest.sequencer.js',
  transform: {
    '^.+\\.(t|j)sx?$': 'ts-jest',
  },
  moduleNameMapper: {
    '^@storage/(.*)$': '<rootDir>/src/storage/$1',
    '^@internal/(.*)$': '<rootDir>/src/internal/$1',
  },
  setupFilesAfterEnv: ['<rootDir>/jest-setup.ts'],
  testEnvironment: 'node',
  testPathIgnorePatterns: ['node_modules', 'dist'],
  coverageProvider: 'v8',
}
