import { describe, expect, it } from 'vitest';
import { normalize } from '../../src/core/normalize.js';
import { createValidator, validateRow } from '../../src/core/validate.js';
import type { RawRecord } from '../../src/core/types.js';
import { getDataset } from '../../src/datasets/registry.js';

const ds = getDataset('customers');
const validator = createValidator(ds.schema);

const good: RawRecord = {
  customer_id: 'C-1',
  email: 'a@b.com',
  country: 'DE',
  currency: 'EUR',
  amount: '12.50',
  contract_start: '2026-01-31',
  plan: 'pro',
};

const check = (r: RawRecord) => validateRow(validator, normalize(r, ds.manifest.columns));

describe('validateRow', () => {
  it('accepts a good row', () => {
    expect(check(good).ok).toBe(true);
  });

  it('accepts null email', () => {
    expect(check({ ...good, email: null }).ok).toBe(true);
    expect(check({ ...good, email: '' }).ok).toBe(true);
  });

  it('accepts messy-but-valid values after normalising', () => {
    const r = check({ ...good, email: '  A@B.COM ', country: 'de', currency: ' eur ', plan: ' Pro ' });
    expect(r.ok).toBe(true);
    if (r.ok) expect(r.row.email).toBe('a@b.com');
  });

  const bad: Array<[string, RawRecord, string]> = [
    ['missing required field', (() => { const { plan: _p, ...rest } = good; return rest; })(), '/plan|'],
    ['customer_id X-1', { ...good, customer_id: 'X-1' }, '/customer_id'],
    ['empty customer_id', { ...good, customer_id: '' }, '/customer_id'],
    ['email foo@', { ...good, email: 'foo@' }, '/email'],
    ['country DEU', { ...good, country: 'DEU' }, '/country'],
    ['currency EURO', { ...good, currency: 'EURO' }, '/currency'],
    ['amount 1,50', { ...good, amount: '1,50' }, '/amount'],
    ['amount 1.505', { ...good, amount: '1.505' }, '/amount'],
    ['date 13/01/2026', { ...good, contract_start: '13/01/2026' }, '/contract_start'],
    ['date 2026-02-30', { ...good, contract_start: '2026-02-30' }, '/contract_start'],
    ['date 2026-13-01', { ...good, contract_start: '2026-13-01' }, '/contract_start'],
    ['plan gold', { ...good, plan: 'gold' }, '/plan'],
    ['extra property', { ...good, surprise: 'x' }, ''],
  ];

  it.each(bad)('rejects %s', (_name, rec, pathHint) => {
    const r = check(rec);
    expect(r.ok).toBe(false);
    if (!r.ok) {
      expect(r.errors.length).toBeGreaterThan(0);
      if (pathHint && !pathHint.includes('|')) expect(r.errors.some((e) => e.instancePath === pathHint)).toBe(true);
    }
  });

  it('reports multiple errors together (allErrors)', () => {
    const r = check({ ...good, country: 'DEU', plan: 'gold', amount: 'x' });
    expect(r.ok).toBe(false);
    if (!r.ok) {
      const paths = r.errors.map((e) => e.instancePath);
      expect(paths).toEqual(expect.arrayContaining(['/country', '/plan', '/amount']));
    }
  });

  it('compiles once per $id', () => {
    expect(createValidator(ds.schema)).toBe(validator);
  });
});
