module.exports = {
  preset: 'ts-jest',
  testSequencer: './jest.sequencer.cjs',
  transform: {
    '^.+/node_modules/jose/.+\\.[jt]s$': 'babel-jest',
    '^.+/node_modules/@tus/.+\\.[jt]s$': 'babel-jest',
    '^.+/node_modules/srvx/.+\\.[jt]s$': 'babel-jest',
    '^.+/node_modules/cookie-es/.+\\.[jt]s$': 'babel-jest',
    '^.+\\.mjs$': 'babel-jest',
    '^.+\\.(t|j)sx?$': 'ts-jest',
  },
  transformIgnorePatterns: ['node_modules/(?!(jose|@tus|srvx|cookie-es)/)'],
  moduleNameMapper: {
    '^@storage/(.*)$': '<rootDir>/src/storage/$1',
    '^@internal/(.*)$': '<rootDir>/src/internal/$1',
  },
  setupFilesAfterEnv: ['<rootDir>/jest-setup.ts'],
  testEnvironment: 'node',
  testPathIgnorePatterns: ['node_modules', 'dist'],
  coverageProvider: 'v8',
}
