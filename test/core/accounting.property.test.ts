import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { checkChunk, checkFile, type ChunkCounts } from '../../src/core/accounting.js';

const chunkArb = fc
  .tuple(fc.nat(10000), fc.nat(10000))
  .map(([v, q]): ChunkCounts => ({ rowsIn: v + q, rowsValid: v, rowsQuarantined: q }));

describe('accounting', () => {
  it('checkFile is ok when every chunk balances', () => {
    fc.assert(
      fc.property(fc.array(chunkArb, { maxLength: 30 }), (chunks) => {
        const total = chunks.reduce((s, c) => s + c.rowsIn, 0);
        expect(checkFile(chunks, total)).toEqual({ ok: true });
      }),
      { numRuns: 500 },
    );
  });

  it('changing any one count by +-1 fails', () => {
    fc.assert(
      fc.property(
        fc.array(chunkArb, { minLength: 1, maxLength: 30 }),
        fc.nat(),
        fc.constantFrom('rowsIn', 'rowsValid', 'rowsQuarantined', 'fileRowsIn'),
        fc.constantFrom(1, -1),
        (chunks, pick, field, delta) => {
          const total = chunks.reduce((s, c) => s + c.rowsIn, 0);
          const copy = chunks.map((c) => ({ ...c }));
          let fileRows = total;
          if (field === 'fileRowsIn') fileRows += delta;
          else copy[pick % copy.length]![field as 'rowsIn'] += delta;
          expect(checkFile(copy, fileRows).ok).toBe(false);
        },
      ),
      { numRuns: 500 },
    );
  });

  it('reason names the chunk and the three numbers', () => {
    const r = checkChunk({ index: 3, rowsIn: 10, rowsValid: 6, rowsQuarantined: 3 });
    expect(r.ok).toBe(false);
    if (!r.ok) expect(r.reason).toMatch(/chunk 3.*10.*6.*3/);
  });
});
