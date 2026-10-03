import { describe, expect, it } from 'vitest';
import { fieldsToRecord } from '../../src/core/csv.js';
import { normalize } from '../../src/core/normalize.js';
import { createValidator, validateRow } from '../../src/core/validate.js';
import { getDataset } from '../../src/datasets/registry.js';
import { CUSTOMERS_HEADER, generateCustomers } from '../../src/fixtures/generate.js';
import { parse } from 'csv-parse/sync';

const ds = getDataset('customers');
const validator = createValidator(ds.schema);

function check(fields: string[]): boolean {
  const { record, errors } = fieldsToRecord(fields, CUSTOMERS_HEADER.split(','));
  if (errors.length) return false;
  return validateRow(validator, normalize(record, ds.manifest.columns)).ok;
}

describe('generateCustomers', () => {
  it('is deterministic for a seed', () => {
    const a = generateCustomers({ rows: 500, badRate: 0.05, seed: 9 });
    const b = generateCustomers({ rows: 500, badRate: 0.05, seed: 9 });
    const c = generateCustomers({ rows: 500, badRate: 0.05, seed: 10 });
    expect(a.csv).toBe(b.csv);
    expect([...a.labels]).toEqual([...b.labels]);
    expect(a.csv).not.toBe(c.csv);
  });

  it('every labelled row fails and every other row passes', () => {
    const { csv, labels } = generateCustomers({ rows: 3000, badRate: 0.1, seed: 4 });
    const recs = parse(csv!, { relax_column_count: true, skip_empty_lines: true }) as string[][];
    expect(recs[0]).toEqual(CUSTOMERS_HEADER.split(','));
    expect(recs.length - 1).toBe(3000);
    expect(labels.size).toBeGreaterThan(0);
    recs.slice(1).forEach((fields, idx) => {
      const rowNumber = idx + 1;
      expect({ rowNumber, ok: check(fields) }).toEqual({ rowNumber, ok: !labels.has(rowNumber) });
    });
  });

  it('bad rate is within 1 percentage point at 10,000 rows', () => {
    const { labels } = generateCustomers({ rows: 10000, badRate: 0.03, seed: 42 });
    expect(Math.abs(labels.size / 10000 - 0.03)).toBeLessThan(0.01);
  });

  it('streams lines through onLine without building a string', () => {
    const lines: string[] = [];
    const r = generateCustomers({ rows: 10, badRate: 0, seed: 1, onLine: (l) => lines.push(l) });
    expect(r.csv).toBeUndefined();
    expect(lines).toHaveLength(11);
    expect(r.labels.size).toBe(0);
  });

  it('exercises messy values and quoted emails', () => {
    const { csv } = generateCustomers({ rows: 5000, badRate: 0, seed: 2 });
    expect(csv).toMatch(/,"user\d+@example\.com",/);
    expect(csv).toMatch(/ User\d+@Example\.COM /);
  });
});
