import { describe, expect, it } from 'vitest';
import { normalize } from '../../src/core/normalize.js';
import type { RawRecord, ValidRow } from '../../src/core/types.js';
import { getDataset } from '../../src/datasets/registry.js';
import { readParquetRows, writeParquet } from '../../src/pipeline/parquet.js';

const cols = getDataset('customers').manifest.columns;
const mk = (i: number, over: RawRecord = {}): ValidRow =>
  normalize(
    {
      customer_id: `C-${i}`,
      email: `u${i}@x.com`,
      country: 'DE',
      currency: 'EUR',
      amount: '12.34',
      contract_start: '2026-02-28',
      plan: 'pro',
      ...over,
    },
    cols,
  ) as unknown as ValidRow;

describe('parquet', () => {
  it('round-trips rows with a null email, negative cents and a date', async () => {
    const rows = [mk(1), mk(2, { email: null, amount: '-7.05' }), mk(3, { contract_start: '2024-02-29', amount: '0' })];
    const back = await readParquetRows(writeParquet(rows, cols));
    expect(back).toHaveLength(3);
    expect(back[0]).toMatchObject({ customer_id: 'C-1', email: 'u1@x.com', amount_cents: 1234n });
    expect(back[1]).toMatchObject({ email: null, amount_cents: -705n });
    expect(back[2]?.contract_start).toEqual(new Date(Date.UTC(2024, 1, 29)));
    expect(back[2]?.amount_cents).toBe(0n);
  });

  it('uses outputName for the amount column', async () => {
    const back = await readParquetRows(writeParquet([mk(1)], cols));
    expect(Object.keys(back[0]!)).toContain('amount_cents');
    expect(Object.keys(back[0]!)).not.toContain('amount');
  });

  it('round-trips 5,000 rows with an exact count', async () => {
    const rows = Array.from({ length: 5000 }, (_, i) => mk(i + 1));
    const back = await readParquetRows(writeParquet(rows, cols));
    expect(back).toHaveLength(5000);
    expect(new Set(back.map((r) => r.customer_id)).size).toBe(5000);
  });
});
