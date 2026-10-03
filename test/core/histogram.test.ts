import { describe, expect, it } from 'vitest';
import { histogram } from '../../src/core/histogram.js';

const e = (instancePath: string, keyword: string) => ({ instancePath, keyword, message: '' });

describe('histogram', () => {
  it('sorts by count desc then path', () => {
    const h = histogram([
      { errors: [e('/plan', 'enum'), e('/email', 'format')] },
      { errors: [e('/plan', 'enum')] },
      { errors: [e('/country', 'pattern')] },
    ]);
    expect(h).toEqual([
      { instancePath: '/plan', keyword: 'enum', count: 2 },
      { instancePath: '/country', keyword: 'pattern', count: 1 },
      { instancePath: '/email', keyword: 'format', count: 1 },
    ]);
  });

  it('is empty for no records', () => {
    expect(histogram([])).toEqual([]);
  });
});
