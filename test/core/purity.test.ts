import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';

function walk(dir: string): string[] {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const p = path.join(dir, e.name);
    return e.isDirectory() ? walk(p) : p.endsWith('.ts') ? [p] : [];
  });
}

describe('src/core purity', () => {
  it('imports no AWS SDK, fs or adapters', () => {
    const root = path.resolve(import.meta.dirname, '../../src/core');
    const bad = /from ['"](@aws-sdk\/|node:fs|\.\.\/adapters)/;
    const offenders = walk(root).filter((f) => bad.test(fs.readFileSync(f, 'utf8')));
    expect(offenders).toEqual([]);
  });
});
