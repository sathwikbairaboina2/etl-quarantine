import { describe, expect, it } from 'vitest';
import { promote } from '../../src/pipeline/promote.js';
import { reconcile } from '../../src/pipeline/reconcile.js';
import { register } from '../../src/pipeline/register.js';
import { split } from '../../src/pipeline/split.js';
import { validateTransform } from '../../src/pipeline/validate-transform.js';
import { StateError } from '../../src/ports.js';
import { memDeps, type MemDeps } from '../support/deps.js';

const HEADER = 'customer_id,email,country,currency,amount,contract_start,plan';
const good = (i: number) => `C-${i},a${i}@x.com,DE,EUR,1.00,2026-01-01,pro`;
const bad = (i: number) => `C-${i},a${i}@x.com,DE,EUR,1.00,13/01/2026,pro`;

/** nGood good rows then nBad bad rows, processed through register, split and every chunk. */
async function ingest(d: MemDeps, nGood: number, nBad: number, skipChunk = false) {
  const rows = [...Array.from({ length: nGood }, (_, i) => good(i + 1)), ...Array.from({ length: nBad }, (_, i) => bad(nGood + i + 1))];
  await d.objects.put('raw', 'dataset=customers/in.csv', `${HEADER}\n${rows.join('\n')}\n`);
  const { sha } = await register(d, { key: 'dataset=customers/in.csv' });
  const s = await split(d, { sha });
  const items = JSON.parse(new TextDecoder().decode(await d.objects.get('staging', s.manifestKey))) as Array<{ index: number; key: string; rows: number; sha: string }>;
  for (const it of items) {
    if (skipChunk && it.index === 1) continue;
    await validateTransform(d, { sha, chunk: it, attempt: 0 });
  }
  return sha;
}

const visible = (d: MemDeps) => d.objects.list('curated', 'dataset=');
const pending = (d: MemDeps) => d.objects.list('curated', '_pending/');

describe('reconcile', () => {
  it('a clean file is LOADED and nothing is left pending', async () => {
    const d = memDeps({ chunkRows: 10 });
    const sha = await ingest(d, 25, 0);
    const r = await reconcile(d, { sha });
    expect(r).toMatchObject({ status: 'LOADED', rowsIn: 25, rowsValid: 25, rowsQuarantined: 0, ratio: 0 });
    expect(await visible(d)).toHaveLength(3);
    expect(await pending(d)).toEqual([]);
    expect(await d.control.getFile(sha)).toMatchObject({ status: 'LOADED', rowsValid: 25 });
  });

  it('a ratio exactly equal to the threshold is not held', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await ingest(d, 19, 1); // 1/20 = 0.05
    const r = await reconcile(d, { sha });
    expect(r.status).toBe('LOADED_WITH_QUARANTINE');
    expect(r.ratio).toBe(0.05);
  });

  it('one row more than the threshold is HELD and invisible', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await ingest(d, 18, 2);
    const r = await reconcile(d, { sha });
    expect(r.status).toBe('HELD');
    expect(await visible(d)).toEqual([]);
    expect(await pending(d)).toHaveLength(1);
  });

  it('promote makes a HELD file visible', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await ingest(d, 18, 2);
    await reconcile(d, { sha });
    await promote(d, { sha });
    expect(await visible(d)).toHaveLength(1);
    expect(await pending(d)).toEqual([]);
    expect(await d.control.getFile(sha)).toMatchObject({ status: 'LOADED_WITH_QUARANTINE', promotedAt: '2026-10-04T09:00:00.000Z' });
  });

  it('promote on a non-HELD file throws', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await ingest(d, 20, 0);
    await reconcile(d, { sha });
    await expect(promote(d, { sha })).rejects.toBeInstanceOf(StateError);
  });

  it('tampered chunk counts FAIL the file and nothing is visible', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await ingest(d, 20, 0);
    await d.control.recordChunkResult(sha, 0, { rowsValid: 19, rowsQuarantined: 0, outputKey: null, quarantineKey: null, attempt: 5 });
    const r = await reconcile(d, { sha });
    expect(r.status).toBe('FAILED');
    expect(r.error).toMatch(/chunk 0/);
    expect(await visible(d)).toEqual([]);
    expect((await d.control.getFile(sha))?.status).toBe('FAILED');
  });

  it('a missing chunk result FAILs the file', async () => {
    const d = memDeps({ chunkRows: 10 });
    const sha = await ingest(d, 25, 0, true);
    const r = await reconcile(d, { sha });
    expect(r).toMatchObject({ status: 'FAILED', error: 'chunk 1 has no result' });
    expect(await visible(d)).toEqual([]);
  });

  it('is stable when run twice', async () => {
    const d = memDeps({ chunkRows: 10 });
    const sha = await ingest(d, 24, 1);
    const a = await reconcile(d, { sha });
    const filesA = await visible(d);
    const b = await reconcile(d, { sha });
    expect(b).toEqual(a);
    expect(await visible(d)).toEqual(filesA);
    expect(await pending(d)).toEqual([]);
  });

  it('an empty file loads with ratio 0', async () => {
    const d = memDeps();
    await d.objects.put('raw', 'dataset=customers/in.csv', `${HEADER}\n`);
    const { sha } = await register(d, { key: 'dataset=customers/in.csv' });
    await split(d, { sha });
    expect(await reconcile(d, { sha })).toMatchObject({ status: 'LOADED', rowsIn: 0, ratio: 0 });
  });
});
