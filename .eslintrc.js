module.exports = {
  ignorePatterns: ['src/test/assets/**', 'src/test/db/**', 'src/test/*.yaml', 'src/**/**/*.md'],
  parser: '@typescript-eslint/parser',
  extends: ['plugin:@typescript-eslint/recommended', 'plugin:prettier/recommended'],
  parserOptions: {
    ecmaVersion: 2020, // Allows for the parsing of modern ECMAScript features
    sourceType: 'module', // Allows for the use of imports
    project: './tsconfig.json',
  },
  rules: {
    '@typescript-eslint/no-floating-promises': 'error',
    '@typescript-eslint/no-explicit-any': 'warn',
    '@typescript-eslint/no-unused-vars': [
      'warn',
      { argsIgnorePattern: '^_+$', varsIgnorePattern: '^_+$' }, // allows intentionally unused variables named _
    ],
    '@typescript-eslint/no-require-imports': 'warn',
  },
}
