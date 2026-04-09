module.exports = {
  roots: ['<rootDir>/src/test'],
  testSequencer: './jest.sequencer.cjs',
  transform: {
    '^.+/node_modules/@aws-sdk/.+\\.[jt]s$': 'babel-jest',
    '^.+/node_modules/@smithy/.+\\.[jt]s$': 'babel-jest',
    '^.+/node_modules/jose/.+\\.[jt]s$': 'babel-jest',
    '^.+/node_modules/@tus/.+\\.[jt]s$': 'babel-jest',
    '^.+/node_modules/srvx/.+\\.[jt]s$': 'babel-jest',
    '^.+/node_modules/cookie-es/.+\\.[jt]s$': 'babel-jest',
    '^.+/node_modules/@kubernetes/client-node/.+\\.[jt]s$': 'babel-jest',
    '^.+/node_modules/oauth4webapi/.+\\.[jt]s$': 'babel-jest',
    '^.+\\.mjs$': 'babel-jest',
    '^.+\\.(t|j)sx?$': [
      'ts-jest',
      {
        diagnostics: false,
        tsconfig: './tsconfig.jest.json',
      },
    ],
  },
  transformIgnorePatterns: [
    'node_modules/(?!(jose|@tus|srvx|cookie-es|@kubernetes|openid-client|oauth4webapi|@aws-sdk|@smithy)/)',
  ],
  moduleNameMapper: {
    '^@storage/(.*)$': '<rootDir>/src/storage/$1',
    '^@internal/(.*)$': '<rootDir>/src/internal/$1',
  },
  setupFilesAfterEnv: ['<rootDir>/src/test/jest-setup.ts'],
  testEnvironment: 'node',
  testMatch: ['**/*.test.ts'],
  testPathIgnorePatterns: ['node_modules', 'dist'],
  coverageReporters: ['lcovonly', 'text-summary'],
  coverageProvider: 'v8',
}
