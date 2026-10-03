import { describe, expect, it } from 'vitest';
import { runIngest } from '../../src/pipeline/driver.js';
import { conservation, readback } from '../../src/pipeline/readback.js';
import { fixtureBytes } from '../support/fixtures.js';
import { memDeps, type MemDeps } from '../support/deps.js';

const KEY = 'dataset=customers/c.csv';

async function ingest(d: MemDeps, plan?: Parameters<typeof runIngest>[2], key = KEY) {
  if (!(await d.objects.head('raw', key))) await d.objects.put('raw', key, fixtureBytes('customers-3pct'));
  return runIngest(d, { key }, plan);
}

describe('driver fault injection', () => {
  it('conserves rows when chunks crash after and before output', async () => {
    const clean = memDeps({ chunkRows: 100 });
    const base = await ingest(clean);
    expect(base).toMatchObject({ status: 'LOADED_WITH_QUARANTINE', chunkCount: 10 });

    const d = memDeps({ chunkRows: 100 });
    const run = await ingest(d, { crashAfterOutput: [2, [5, 2]], crashBeforeOutput: [7] });
    expect(run.status).toBe('LOADED_WITH_QUARANTINE');
    expect(run).toMatchObject({ rowsIn: base.rowsIn, rowsValid: base.rowsValid, rowsQuarantined: base.rowsQuarantined });
    expect(run.attempts[2]).toBe(2);
    expect(run.attempts[5]).toBe(3);
    expect(run.attempts[7]).toBe(2);
    expect(run.attempts[0]).toBe(1);

    const rb = await readback(d.objects, 'customers');
    expect(rb.parquetRows).toBe(run.rowsValid);
    expect(rb.distinctIds).toBe(run.rowsValid);
    expect(rb.quarantineRows).toBe(run.rowsQuarantined);
    expect(conservation(rb, run.rowsIn)).toMatchObject({ lost: 0, duplicated: 0, conserved: true });
    // identical to the no-fault run
    expect(rb).toEqual(await readback(clean.objects, 'customers'));
  });

  it('a chunk failing every attempt marks the file FAILED with nothing visible', async () => {
    const d = memDeps({ chunkRows: 100 });
    const run = await ingest(d, { crashAfterOutput: [[3, 3]] });
    expect(run.status).toBe('FAILED');
    expect(run.error).toMatch(/chunk 3 failed after 3 attempts/);
    expect((await d.control.getFile(run.sha))?.status).toBe('FAILED');
    expect((await readback(d.objects, 'customers')).parquetRows).toBe(0);
  });

  it('a second delivery of the same bytes is a DUPLICATE and changes nothing', async () => {
    const d = memDeps({ chunkRows: 100 });
    const first = await ingest(d);
    const before = await readback(d.objects, 'customers');
    await d.objects.put('raw', 'dataset=customers/copy.csv', fixtureBytes('customers-3pct'));
    const second = await runIngest(d, { key: 'dataset=customers/copy.csv' });
    expect(second).toMatchObject({ status: 'DUPLICATE', sha: first.sha, originalKey: KEY });
    expect(await readback(d.objects, 'customers')).toEqual(before);
    // same key again
    expect((await runIngest(d, { key: KEY })).status).toBe('DUPLICATE');
    expect((await d.control.getFile(first.sha))?.status).toBe('LOADED_WITH_QUARANTINE');
  });

  it('negative control: deleting a visible Parquet part is detected as lost rows', async () => {
    const d = memDeps({ chunkRows: 100 });
    const run = await ingest(d);
    const parts = await d.objects.list('curated', 'dataset=customers/');
    expect(parts.length).toBeGreaterThan(1);
    await d.objects.delete('curated', parts[0]!);
    const c = conservation(await readback(d.objects, 'customers'), run.rowsIn);
    expect(c.lost).toBeGreaterThan(0);
    expect(c.conserved).toBe(false);
  });

  it('negative control: a duplicated part is detected as duplicated rows', async () => {
    const d = memDeps({ chunkRows: 100 });
    const run = await ingest(d);
    const parts = await d.objects.list('curated', 'dataset=customers/');
    await d.objects.put('curated', 'dataset=customers/ingest_date=2026-10-04/copy-00000.parquet', await d.objects.get('curated', parts[0]!));
    const c = conservation(await readback(d.objects, 'customers'), run.rowsIn);
    expect(c.duplicated).toBeGreaterThan(0);
    expect(c.conserved).toBe(false);
  });

  it('a split failure surfaces as FAILED without chunks', async () => {
    const d = memDeps();
    await d.objects.put('raw', 'dataset=customers/bad.csv', 'a,b\n1,2\n');
    const run = await runIngest(d, { key: 'dataset=customers/bad.csv' });
    expect(run.status).toBe('FAILED');
    expect(run.error).toMatch(/header mismatch/);
  });
});
