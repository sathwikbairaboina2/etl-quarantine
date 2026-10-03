import { describe, expect, it } from 'vitest';
import { checkHeader, fieldsToRecord, HeaderError } from '../../src/core/csv.js';
import { getDataset } from '../../src/datasets/registry.js';

const m = getDataset('customers').manifest;
const cols = m.columns.map((c) => c.name);

describe('checkHeader', () => {
  it('accepts the manifest order', () => {
    expect(checkHeader(cols, m)).toEqual(cols);
  });
  it('accepts a reordered header and returns file order', () => {
    const re = [...cols].reverse();
    expect(checkHeader(re, m)).toEqual(re);
  });
  it('strips BOM and whitespace', () => {
    const h = [...cols];
    h[0] = '﻿customer_id';
    h[1] = ' email ';
    expect(checkHeader(h, m)).toEqual(cols);
  });
  it('rejects a missing column', () => {
    expect(() => checkHeader(cols.slice(1), m)).toThrow(/missing columns: customer_id/);
  });
  it('rejects an extra column', () => {
    expect(() => checkHeader([...cols, 'bonus'], m)).toThrow(HeaderError);
    expect(() => checkHeader([...cols, 'bonus'], m)).toThrow(/unexpected columns: "bonus"/);
  });
  it('is case-sensitive', () => {
    const h = [...cols];
    h[0] = 'Customer_ID';
    expect(() => checkHeader(h, m)).toThrow(HeaderError);
  });
});

describe('fieldsToRecord', () => {
  it('maps a full row', () => {
    const r = fieldsToRecord(['a', 'b', 'c'], ['x', 'y', 'z']);
    expect(r.record).toEqual({ x: 'a', y: 'b', z: 'c' });
    expect(r.errors).toEqual([]);
  });
  it('flags a short row and nulls the missing fields', () => {
    const r = fieldsToRecord(['a'], ['x', 'y', 'z']);
    expect(r.record).toEqual({ x: 'a', y: null, z: null });
    expect(r.errors).toEqual([{ instancePath: '', keyword: 'columnCount', message: 'expected 3 fields, got 1' }]);
  });
  it('flags a long row', () => {
    const r = fieldsToRecord(['a', 'b', 'c', 'd'], ['x', 'y', 'z']);
    expect(r.record).toEqual({ x: 'a', y: 'b', z: 'c' });
    expect(r.errors[0]?.message).toBe('expected 3 fields, got 4');
  });
});
