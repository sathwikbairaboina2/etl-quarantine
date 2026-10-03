import type { Limits } from './types.js';

export class LimitError extends Error {
  override name = 'LimitError';
}

export const DEFAULT_LIMITS: Limits = {
  maxFileBytes: 5 * 1024 ** 3,
  maxRowBytes: 1024 ** 2,
  maxColumns: 200,
};

export function checkFileSize(size: number, limits: Limits): void {
  if (size > limits.maxFileBytes) {
    throw new LimitError(`file size ${size} bytes exceeds limit ${limits.maxFileBytes} bytes`);
  }
}

export function checkColumns(n: number, limits: Limits): void {
  if (n > limits.maxColumns) {
    throw new LimitError(`${n} columns exceeds limit ${limits.maxColumns} columns`);
  }
}
