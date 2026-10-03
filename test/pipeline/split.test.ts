import { describe, expect, it } from 'vitest';
import { stagingChunkKey } from '../../src/core/keys.js';
import { register } from '../../src/pipeline/register.js';
import { split } from '../../src/pipeline/split.js';
import { memDeps, type MemDeps } from '../support/deps.js';

const HEADER = 'customer_id,email,country,currency,amount,contract_start,plan';
const row = (i: number) => `C-${i},a${i}@x.com,DE,EUR,1.00,2026-01-01,pro`;

async function setup(d: MemDeps, body: string | Uint8Array) {
  const key = 'dataset=customers/in.csv';
  await d.objects.put('raw', key, body);
  const r = await register(d, { key });
  return r.sha;
}

async function stagedLines(d: MemDeps, sha: string, i: number) {
  const text = new TextDecoder().decode(await d.objects.get('staging', stagingChunkKey(sha, i)));
  return text
    .split('\n')
    .filter(Boolean)
    .map((l) => JSON.parse(l) as { rowNumber: number; raw: string; fields: string[] });
}

describe('split', () => {
  it('splits 12 rows with chunkRows 5 into 5/5/2 with correct row numbers', async () => {
    const d = memDeps({ chunkRows: 5 });
    {
      const body = [HEADER, ...Array.from({ length: 12 }, (_, i) => row(i + 1))].join('\n') + '\n';
      const sha = await setup(d, body);
      const r = await split(d, { sha });
      expect(r).toMatchObject({ status: 'SPLIT', chunkCount: 3, rowsIn: 12 });
      expect((await stagedLines(d, sha, 0)).map((l) => l.rowNumber)).toEqual([1, 2, 3, 4, 5]);
      expect((await stagedLines(d, sha, 2)).map((l) => l.rowNumber)).toEqual([11, 12]);
      const chunks = await d.control.listChunks(sha);
      expect(chunks.map((c) => c.rowsIn)).toEqual([5, 5, 2]);
      const items = JSON.parse(new TextDecoder().decode(await d.objects.get('staging', r.manifestKey)));
      expect(items).toHaveLength(3);
      expect(items[1]).toMatchObject({ sha, index: 1, rows: 5 });
      const meta = await d.control.getFile(sha);
      expect(meta).toMatchObject({ status: 'SPLIT', rowsIn: 12, chunkCount: 3 });
      expect(meta?.columns).toEqual(HEADER.split(','));
    }
  });

  it('handles BOM, CRLF, quoted comma and quoted newline, keeping raw text', async () => {
    const d = memDeps();
    const body =
      '﻿' + HEADER + '\r\n' +
      'C-1,"a,b@x.com",DE,EUR,1.00,2026-01-01,pro\r\n' +
      'C-2,"multi\nline@x.com",DE,EUR,2.00,2026-01-01,pro\r\n' +
      '\r\n';
    const sha = await setup(d, body);
    const r = await split(d, { sha });
    expect(r).toMatchObject({ status: 'SPLIT', rowsIn: 2 });
    const lines = await stagedLines(d, sha, 0);
    expect(lines).toHaveLength(2);
    expect(lines[0]?.fields[1]).toBe('a,b@x.com');
    expect(lines[0]?.raw).toBe('C-1,"a,b@x.com",DE,EUR,1.00,2026-01-01,pro');
    expect(lines[1]?.fields[1]).toBe('multi\nline@x.com');
    expect(lines[1]?.raw).toBe('C-2,"multi\nline@x.com",DE,EUR,2.00,2026-01-01,pro');
  });

  it('does not count a trailing blank line', async () => {
    const d = memDeps();
    const sha = await setup(d, `${HEADER}\n${row(1)}\n\n\n`);
    expect((await split(d, { sha })).rowsIn).toBe(1);
  });

  it('passes ragged rows through to staging', async () => {
    const d = memDeps();
    const sha = await setup(d, `${HEADER}\n${row(1)}\nC-2,only\n${row(3)},extra\n`);
    const r = await split(d, { sha });
    expect(r).toMatchObject({ status: 'SPLIT', rowsIn: 3 });
    const lines = await stagedLines(d, sha, 0);
    expect(lines.map((l) => l.fields.length)).toEqual([7, 2, 8]);
  });

  it('fails an unclosed quote at EOF with no chunks visible', async () => {
    const d = memDeps();
    const sha = await setup(d, `${HEADER}\n${row(1)}\nC-2,"unclosed,DE,EUR,1.00,2026-01-01,pro\n`);
    const r = await split(d, { sha });
    expect(r.status).toBe('FAILED');
    expect(r.error).toMatch(/quote|Quote/i);
    expect(await d.objects.list('staging', sha)).toEqual([]);
    expect(await d.control.listChunks(sha)).toEqual([]);
    expect(await d.control.getFile(sha)).toMatchObject({ status: 'FAILED' });
  });

  it('fails a row over maxRowBytes', async () => {
    const d = memDeps({ limits: { maxFileBytes: 1e9, maxRowBytes: 64, maxColumns: 200 } });
    const sha = await setup(d, `${HEADER}\nC-1,${'x'.repeat(200)},DE,EUR,1.00,2026-01-01,pro\n`);
    const r = await split(d, { sha });
    expect(r.status).toBe('FAILED');
  });

  it('fails 201 columns', async () => {
    const d = memDeps();
    const cols = Array.from({ length: 201 }, (_, i) => `c${i}`).join(',');
    const sha = await setup(d, `${cols}\n1\n`);
    const r = await split(d, { sha });
    expect(r.status).toBe('FAILED');
    expect(r.error).toMatch(/201.*200/);
  });

  it('fails a header mismatch', async () => {
    const d = memDeps();
    const sha = await setup(d, 'customer_id,email\nC-1,a@b.com\n');
    const r = await split(d, { sha });
    expect(r.status).toBe('FAILED');
    expect(r.error).toMatch(/missing columns/);
  });

  it('fails an empty file', async () => {
    const d = memDeps();
    const sha = await setup(d, 'x');
    await d.objects.put('raw', 'dataset=customers/in.csv', '');
    expect((await split(d, { sha })).status).toBe('FAILED');
  });

  it('splits a header-only file into zero chunks', async () => {
    const d = memDeps();
    const sha = await setup(d, `${HEADER}\n`);
    const r = await split(d, { sha });
    expect(r).toMatchObject({ status: 'SPLIT', chunkCount: 0, rowsIn: 0 });
    expect(JSON.parse(new TextDecoder().decode(await d.objects.get('staging', r.manifestKey)))).toEqual([]);
  });
});
