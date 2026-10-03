import { describe, expect, it } from 'vitest';
import { normalize } from '../../src/core/normalize.js';
import { getDataset } from '../../src/datasets/registry.js';

const cols = getDataset('customers').manifest.columns;

describe('normalize', () => {
  it('applies ops in order', () => {
    const out = normalize(
      { customer_id: ' C-1 ', email: '  A@B.COM ', country: ' de', currency: ' eur', amount: ' 1.50 ', contract_start: ' 2026-01-01', plan: ' Pro ' },
      cols,
    );
    expect(out).toEqual({
      customer_id: 'C-1',
      email: 'a@b.com',
      country: 'DE',
      currency: 'EUR',
      amount: '1.50',
      contract_start: '2026-01-01',
      plan: 'pro',
    });
  });

  it('turns empty email into null and skips null for later ops', () => {
    expect(normalize({ email: '   ' }, cols).email).toBeNull();
    expect(normalize({ email: null }, cols).email).toBeNull();
  });

  it('leaves empty required strings as empty (validation rejects them)', () => {
    expect(normalize({ customer_id: '' }, cols).customer_id).toBe('');
  });
});
