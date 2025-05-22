module.exports = {
  preset: 'ts-jest',
  testSequencer: './jest.sequencer.cjs',
  transform: {
    '^.+/node_modules/jose/.+\\.[jt]s$': 'babel-jest',
    '^.+\\.mjs$': 'babel-jest',
    '^.+\\.(t|j)sx?$': 'ts-jest',
  },
  transformIgnorePatterns: ['node_modules/(?!(jose)/)'],
  moduleNameMapper: {
    '^@storage/(.*)$': '<rootDir>/src/storage/$1',
    '^@internal/(.*)$': '<rootDir>/src/internal/$1',
  },
  setupFilesAfterEnv: ['<rootDir>/jest-setup.ts'],
  testEnvironment: 'node',
  testPathIgnorePatterns: ['node_modules', 'dist'],
  coverageProvider: 'v8',
}
