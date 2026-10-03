import { describe, expect, it } from 'vitest';
import { cast, parseCents } from '../../src/core/cast.js';
import { InvariantError, type ValidRow } from '../../src/core/types.js';
import { getDataset } from '../../src/datasets/registry.js';

const cols = getDataset('customers').manifest.columns;

const row = (over: Record<string, string | null> = {}) =>
  ({
    customer_id: 'C-1',
    email: null,
    country: 'DE',
    currency: 'EUR',
    amount: '1.5',
    contract_start: '2026-02-28',
    plan: 'pro',
    ...over,
  }) as unknown as ValidRow;

describe('parseCents', () => {
  it.each([
    ['0', 0n],
    ['12', 1200n],
    ['12.3', 1230n],
    ['1.5', 150n],
    ['-7.05', -705n],
    ['-0.05', -5n],
    ['999999999999.99', 99999999999999n],
  ])('%s -> %s', (s, n) => {
    expect(parseCents(s)).toBe(n);
  });
});

describe('cast', () => {
  it('casts types and renames amount', () => {
    const out = cast(row(), cols);
    expect(out.amount_cents).toBe(150n);
    expect(out.contract_start).toEqual(new Date(Date.UTC(2026, 1, 28)));
    expect(out.email).toBeNull();
    expect('amount' in out).toBe(false);
  });

  it('throws InvariantError on forged bad rows', () => {
    expect(() => cast(row({ amount: '1,50' }), cols)).toThrow(InvariantError);
    expect(() => cast(row({ contract_start: '2026-02-30' }), cols)).toThrow(InvariantError);
    expect(() => cast(row({ customer_id: null }), cols)).toThrow(InvariantError);
  });
});
