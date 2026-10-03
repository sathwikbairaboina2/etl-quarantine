import { describe, expect, it } from 'vitest';
import { NotFoundError } from '../../src/ports.js';
import { sha256Bytes } from '../../src/pipeline/hash.js';
import { register } from '../../src/pipeline/register.js';
import { memDeps } from '../support/deps.js';

const csv = 'customer_id,email\nC-1,a@b.com\n';
const A = 'dataset=customers/a.csv';

describe('register', () => {
  it('registers a first file with ingestDate from the clock', async () => {
    const d = memDeps();
    await d.objects.put('raw', A, csv);
    const r = await register(d, { key: A });
    expect(r).toMatchObject({ status: 'REGISTERED', dataset: 'customers', sha: sha256Bytes(csv) });
    const meta = await d.control.getFile(r.sha);
    expect(meta).toMatchObject({ status: 'REGISTERED', ingestDate: '2026-10-04', schemaVersion: 'customers/v1', sourceKey: A });
    expect(meta?.parentSha).toBeUndefined();
  });

  it('same bytes under a second key is a DUPLICATE with originalKey', async () => {
    const d = memDeps();
    await d.objects.put('raw', A, csv);
    await d.objects.put('raw', 'dataset=customers/b.csv', csv);
    await register(d, { key: A });
    const r = await register(d, { key: 'dataset=customers/b.csv' });
    expect(r).toMatchObject({ status: 'DUPLICATE', originalKey: A });
    expect((await d.control.getFile(r.sha))?.sourceKey).toBe(A);
    expect(d.control.duplicates).toHaveLength(1);
  });

  it('same key delivered twice is a DUPLICATE', async () => {
    const d = memDeps();
    await d.objects.put('raw', A, csv);
    await register(d, { key: A });
    expect((await register(d, { key: A })).status).toBe('DUPLICATE');
  });

  it('a FAILED original is re-admitted with its chunk records cleared', async () => {
    const d = memDeps();
    await d.objects.put('raw', A, csv);
    await d.objects.put('raw', 'dataset=customers/b.csv', csv);
    const r = await register(d, { key: A });
    await d.control.putChunk(r.sha, { index: 0, key: 'k0', rowsIn: 1 });
    await d.control.updateFile(r.sha, { status: 'FAILED', error: 'x', rowsIn: 1 });
    const again = await register(d, { key: 'dataset=customers/b.csv' });
    expect(again).toMatchObject({ status: 'REGISTERED', sha: r.sha });
    expect(await d.control.getFile(r.sha)).toMatchObject({ status: 'REGISTERED', sourceKey: 'dataset=customers/b.csv' });
    expect((await d.control.getFile(r.sha))?.error).toBeUndefined();
    expect(await d.control.listChunks(r.sha)).toEqual([]);
    // Once re-admitted and not failed, a further delivery is a duplicate again.
    expect((await register(d, { key: A })).status).toBe('DUPLICATE');
  });

  it('size at limit is ok and limit+1 FAILED without META', async () => {
    const d = memDeps({ limits: { maxFileBytes: 10, maxRowBytes: 1024, maxColumns: 200 } });
    await d.objects.put('raw', 'dataset=customers/ok.csv', '0123456789');
    await d.objects.put('raw', 'dataset=customers/big.csv', '0123456789A');
    expect((await register(d, { key: 'dataset=customers/ok.csv' })).status).toBe('REGISTERED');
    const r = await register(d, { key: 'dataset=customers/big.csv' });
    expect(r.status).toBe('FAILED');
    expect(r.error).toMatch(/11.*10/);
    expect(await d.control.getFile(sha256Bytes('0123456789A'))).toBeUndefined();
  });

  it('FAILED for an unknown dataset or a key without dataset=', async () => {
    const d = memDeps();
    await d.objects.put('raw', 'dataset=nope/a.csv', csv);
    await d.objects.put('raw', 'stray.csv', csv);
    expect((await register(d, { key: 'dataset=nope/a.csv' })).status).toBe('FAILED');
    expect((await register(d, { key: 'stray.csv' })).status).toBe('FAILED');
  });

  it('throws NotFoundError for a missing object', async () => {
    await expect(register(memDeps(), { key: 'dataset=customers/none.csv' })).rejects.toBeInstanceOf(NotFoundError);
  });

  it('a replay key sets parentSha', async () => {
    const d = memDeps();
    const k = 'dataset=customers/_replay/abc123-r1.csv';
    await d.objects.put('raw', k, csv);
    const r = await register(d, { key: k });
    expect((await d.control.getFile(r.sha))?.parentSha).toBe('abc123');
  });
});
