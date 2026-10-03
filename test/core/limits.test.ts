import { describe, expect, it } from 'vitest';
import { checkColumns, checkFileSize, DEFAULT_LIMITS, LimitError } from '../../src/core/limits.js';

describe('limits', () => {
  it('has the documented defaults', () => {
    expect(DEFAULT_LIMITS).toEqual({ maxFileBytes: 5 * 1024 ** 3, maxRowBytes: 1024 ** 2, maxColumns: 200 });
  });

  it('file size: at limit ok, limit+1 throws with both numbers', () => {
    const l = DEFAULT_LIMITS;
    expect(() => checkFileSize(l.maxFileBytes, l)).not.toThrow();
    expect(() => checkFileSize(l.maxFileBytes + 1, l)).toThrow(LimitError);
    expect(() => checkFileSize(l.maxFileBytes + 1, l)).toThrow(String(l.maxFileBytes + 1));
  });

  it('columns: 200 ok, 201 throws', () => {
    expect(() => checkColumns(200, DEFAULT_LIMITS)).not.toThrow();
    expect(() => checkColumns(201, DEFAULT_LIMITS)).toThrow(/201.*200/);
  });
});
