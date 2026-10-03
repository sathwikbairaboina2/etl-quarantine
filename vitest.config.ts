import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts', 'infra/test/**/*.test.ts'],
    testTimeout: 120000,
    hookTimeout: 180000,
    pool: 'forks',
  },
});
