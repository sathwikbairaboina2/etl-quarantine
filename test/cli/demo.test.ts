import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterAll, describe, expect, it } from 'vitest';
import { CliError } from '../../src/cli/commands.js';
import { accountRows, runDemo } from '../../src/cli/demo.js';
import { runIngest } from '../../src/pipeline/driver.js';
import { fix } from '../../src/pipeline/fix.js';
import { promote } from '../../src/pipeline/promote.js';
import { replay } from '../../src/pipeline/replay.js';
import { memDeps } from '../support/deps.js';

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

  it('accountRows stays at lost 0 when a replay child quarantines a row of its own', async () => {
    const d = memDeps({ chunkRows: 100 });
    const nl = String.fromCharCode(10);
    const header = 'customer_id,email,country,currency,amount,contract_start,plan';
    const bad = new Set(Array.from({ length: 20 }, (_, i) => (i + 1) * 20));
    const rows = Array.from({ length: 400 }, (_, i) =>
      `C-${i + 1},a${i + 1}@x.com,DE,EUR,1.00,${bad.has(i + 1) ? '13/01/2026' : '2026-01-01'},pro`,
    );
    await d.objects.put('raw', 'dataset=customers/a.csv', [header, ...rows, ''].join(nl));
    const run = await runIngest(d, { key: 'dataset=customers/a.csv' });
    for (const r of [20, 40, 60]) await fix(d, { sha: run.sha, rowNumber: r, set: { contract_start: '2026-01-13' } });
    await fix(d, { sha: run.sha, rowNumber: 80, set: { contract_start: '2026-02-30' } });
    const rep = await replay(d, { sha: run.sha, onlyFixed: true });
    if ('nothingToReplay' in rep) throw new Error('expected a replay');
    const child = await runIngest(d, { key: rep.childKey });
    expect(child).toMatchObject({ status: 'HELD', rowsQuarantined: 1 });
    expect(await accountRows(d, run.sha, 400)).toEqual({ curatedRows: 380, lost: 0 });
    await promote(d, { sha: child.sha });
    expect(await accountRows(d, run.sha, 400)).toEqual({ curatedRows: 383, lost: 0 });
  });
});
