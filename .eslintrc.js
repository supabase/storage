module.exports = {
  ignorePatterns: ['src/test/assets/**', 'src/test/db/**', 'src/test/*.yaml'],
  parser: '@typescript-eslint/parser',
  extends: ['plugin:@typescript-eslint/recommended', 'plugin:prettier/recommended'],
  parserOptions: {
    ecmaVersion: 2020, // Allows for the parsing of modern ECMAScript features
    sourceType: 'module', // Allows for the use of imports
  },
  rules: {
    '@typescript-eslint/no-explicit-any': 'warn',
    '@typescript-eslint/no-unused-vars': 'warn',
    '@typescript-eslint/no-require-imports': 'warn',
  },
}
