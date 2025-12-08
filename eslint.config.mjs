import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import prettierConfig from 'eslint-config-prettier';
import importPlugin from 'eslint-plugin-import';
import globals from 'globals';

export default [
  js.configs.recommended,
  ...tseslint.configs.recommended,
  prettierConfig,
  {
    files: ['**/*.{js,ts}'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      parser: tseslint.parser,
      globals: {
        ...globals.node,
        ...globals.browser,
      },
    },
    plugins: {
      '@typescript-eslint': tseslint.plugin,
      import: importPlugin,
    },
    settings: {
      'import/resolver': {
        node: {
          paths: ['src'],
          extensions: ['.ts', '.js'],
        },
      },
    },
    rules: {
      camelcase: [
        'error',
        {
          ignoreImports: true,
          allow: ['compute_v1'],
        },
      ],
      'import/extensions': 'off',
      'max-classes-per-file': 'off',
      'max-len': 'off',
      'no-await-in-loop': 'off',
      'no-continue': 'off',
      'no-plusplus': 'off',
      'no-restricted-syntax': 'off',
      'no-shadow': 'off',
      'no-use-before-define': 'off',
      'no-useless-constructor': 'off',
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          argsIgnorePattern: '^_',
          varsIgnorePattern: '^_',
        },
      ],
    },
  },
  {
    files: [
      '**/__tests__/**/*.{js,ts}',
      '**/*.test.{js,ts}',
      '**/*.spec.{js,ts}',
    ],
    languageOptions: {
      globals: {
        ...globals.jest,
      },
    },
    rules: {
      '@typescript-eslint/no-require-imports': 'off',
    },
  },
  {
    ignores: ['dist/**', 'node_modules/**', 'coverage/**'],
  },
];
