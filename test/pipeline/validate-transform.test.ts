import { describe, expect, it } from 'vitest';
import { curatedPendingKey, quarantineChunkKey } from '../../src/core/keys.js';
import type { QuarantineRecord } from '../../src/core/types.js';
import { readParquetRows } from '../../src/pipeline/parquet.js';
import { register } from '../../src/pipeline/register.js';
import { split } from '../../src/pipeline/split.js';
import { validateTransform } from '../../src/pipeline/validate-transform.js';
import { memDeps, type MemDeps } from '../support/deps.js';

const HEADER = 'customer_id,email,country,currency,amount,contract_start,plan';
const good = (i: number) => `C-${i},a${i}@x.com,DE,EUR,1.00,2026-01-01,pro`;
const bad = (i: number) => `C-${i},a${i}@x.com,DE,EUR,1.00,13/01/2026,pro`;

async function prepare(d: MemDeps, rows: string[]) {
  await d.objects.put('raw', 'dataset=customers/in.csv', `${HEADER}\n${rows.join('\n')}\n`);
  const { sha } = await register(d, { key: 'dataset=customers/in.csv' });
  const s = await split(d, { sha });
  const items = JSON.parse(new TextDecoder().decode(await d.objects.get('staging', s.manifestKey)));
  return { sha, item: items[0] as { sha: string; index: number; key: string; rows: number } };
}

const mixed = () => [1, 2, 3, 4, 5, 6, 7].map(good).concat([8, 9, 10].map(bad));

describe('validateTransform', () => {
  it('writes only the good rows to Parquet and the bad rows to quarantine', async () => {
    const d = memDeps();
    const { sha, item } = await prepare(d, mixed());
    const r = await validateTransform(d, { sha, chunk: item, attempt: 0 });
    expect(r).toMatchObject({ rowsIn: 10, rowsValid: 7, rowsQuarantined: 3, attempt: 0 });

    const pq = await readParquetRows(await d.objects.get('curated', curatedPendingKey('customers', '2026-10-04', sha, 0)));
    expect(pq.map((x) => x.customer_id).sort()).toEqual([1, 2, 3, 4, 5, 6, 7].map((i) => `C-${i}`).sort());
    expect(pq.some((x) => ['C-8', 'C-9', 'C-10'].includes(String(x.customer_id)))).toBe(false);

    const q = new TextDecoder()
      .decode(await d.objects.get('quarantine', quarantineChunkKey('customers', sha, 0)))
      .split('\n')
      .filter(Boolean)
      .map((l) => JSON.parse(l) as QuarantineRecord);
    expect(q.map((x) => x.rowNumber)).toEqual([8, 9, 10]);
    expect(q[0]?.raw).toBe(bad(8));
    expect(q[0]?.errors.some((e) => e.instancePath === '/contract_start' && e.keyword === 'format')).toBe(true);
    expect(q[0]).toMatchObject({ fileSha: sha, chunk: 0, schemaVersion: 'customers/v1' });

    const [c] = await d.control.listChunks(sha);
    expect(c).toMatchObject({ rowsValid: 7, rowsQuarantined: 3, attempt: 0 });
  });

  it('quarantines ragged rows as columnCount errors', async () => {
    const d = memDeps();
    const { sha, item } = await prepare(d, [good(1), 'C-2,short', `${good(3)},extra`]);
    const r = await validateTransform(d, { sha, chunk: item, attempt: 0 });
    expect(r).toMatchObject({ rowsValid: 1, rowsQuarantined: 2 });
    const q = new TextDecoder().decode(await d.objects.get('quarantine', quarantineChunkKey('customers', sha, 0)));
    expect(q).toContain('columnCount');
  });

  it('writes no Parquet for a chunk of only bad rows', async () => {
    const d = memDeps();
    const { sha, item } = await prepare(d, [bad(1), bad(2)]);
    const r = await validateTransform(d, { sha, chunk: item, attempt: 0 });
    expect(r.outputKey).toBeNull();
    expect(await d.objects.list('curated', '')).toEqual([]);
    expect(r.quarantineKey).not.toBeNull();
  });

  it('is idempotent across attempts: one object, same bytes, counts set not added', async () => {
    const d = memDeps();
    const { sha, item } = await prepare(d, mixed());
    await validateTransform(d, { sha, chunk: item, attempt: 0 });
    const key = curatedPendingKey('customers', '2026-10-04', sha, 0);
    const first = await d.objects.get('curated', key);
    await validateTransform(d, { sha, chunk: item, attempt: 1 });
    expect(await d.objects.list('curated', '')).toEqual([key]);
    expect(await d.objects.get('curated', key)).toEqual(first);
    const [c] = await d.control.listChunks(sha);
    expect(c).toMatchObject({ rowsValid: 7, rowsQuarantined: 3, attempt: 1 });
  });

  it('ignores a stale attempt arriving after a newer one', async () => {
    const d = memDeps();
    const { sha, item } = await prepare(d, mixed());
    await validateTransform(d, { sha, chunk: item, attempt: 1 });
    await validateTransform(d, { sha, chunk: item, attempt: 0 });
    const [c] = await d.control.listChunks(sha);
    expect(c?.attempt).toBe(1);
  });

  it('an afterOutput crash leaves output written but no counts recorded', async () => {
    const d = memDeps({
      faults: {
        afterOutput() {
          throw new Error('boom');
        },
      },
    });
    const { sha, item } = await prepare(d, mixed());
    await expect(validateTransform(d, { sha, chunk: item, attempt: 0 })).rejects.toThrow('boom');
    expect(await d.objects.list('curated', '')).toHaveLength(1);
    const [c] = await d.control.listChunks(sha);
    expect(c?.attempt).toBeUndefined();
    expect(c?.rowsValid).toBeUndefined();
  });

  it('a beforeOutput crash writes nothing', async () => {
    const d = memDeps({
      faults: {
        beforeOutput() {
          throw new Error('early');
        },
      },
    });
    const { sha, item } = await prepare(d, mixed());
    await expect(validateTransform(d, { sha, chunk: item, attempt: 0 })).rejects.toThrow('early');
    expect(await d.objects.list('curated', '')).toEqual([]);
    expect(await d.objects.list('quarantine', '')).toEqual([]);
  });
});
