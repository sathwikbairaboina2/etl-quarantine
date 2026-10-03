import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { isSuperseded, summarize, type RowState } from '../../src/core/lineage.js';

const stateArb = fc.constantFrom<RowState | undefined>('pending', 'loaded', 'requarantined', 'discarded', undefined);

describe('lineage', () => {
  it('every quarantined row is in exactly one state; superseded matches its definition', () => {
    fc.assert(
      fc.property(
        fc.uniqueArray(fc.integer({ min: 1, max: 5000 }), { maxLength: 200 }),
        fc.array(stateArb, { minLength: 200, maxLength: 200 }),
        (rows, assigned) => {
          const states = new Map<number, RowState>();
          rows.forEach((r, i) => {
            const s = assigned[i];
            if (s) states.set(r, s);
          });
          const s = summarize(rows, states);
          expect(s.pending + s.loaded + s.requarantined + s.discarded).toBe(rows.length);
          expect(isSuperseded(s)).toBe(rows.length > 0 && s.pending + s.requarantined === 0);
        },
      ),
      { numRuns: 500 },
    );
  });

  it('missing rows count as pending', () => {
    expect(summarize([1, 2], new Map())).toEqual({ pending: 2, loaded: 0, requarantined: 0, discarded: 0 });
  });

  it('is not superseded with no quarantined rows', () => {
    expect(isSuperseded(summarize([], new Map()))).toBe(false);
  });
});
