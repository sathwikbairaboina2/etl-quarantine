import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterAll, describe, expect, it } from 'vitest';
import { CliError } from '../../src/cli/commands.js';
import { runDemo } from '../../src/cli/demo.js';

const dirs: string[] = [];
afterAll(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

describe('demo', () => {
  it('runs end to end with nothing lost or duplicated', async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'etlq-demo-test-'));
    dirs.push(base);
    const lines: string[] = [];
    const r = await runDemo({ rows: 2000, root: path.join(base, 'demo') }, (l) => lines.push(l));
    const text = lines.join('\n');
    expect(text).toContain('DUPLICATE');
    expect(text).toContain('chunk 2: attempt 1 crashed, attempt 2 ok');
    expect(text).toContain('lost 0');
    expect(text).toContain('duplicated 0');
    expect(r).toMatchObject({ sourceRows: 2000, lost: 0, duplicated: 0 });
    expect(r.loadedFromQuarantine).toBeGreaterThan(0);
    expect(r.curatedRows + r.stillQuarantined + r.discarded).toBe(2000);
  });

  it('refuses a non-empty root', async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'etlq-demo-test-'));
    dirs.push(base);
    fs.writeFileSync(path.join(base, 'keep.txt'), 'x');
    await expect(runDemo({ rows: 100, root: base }, () => {})).rejects.toBeInstanceOf(CliError);
    expect(fs.existsSync(path.join(base, 'keep.txt'))).toBe(true);
  });
});
