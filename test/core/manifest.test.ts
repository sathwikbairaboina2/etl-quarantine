import { describe, expect, it } from 'vitest';
import { ManifestError, parseManifest } from '../../src/core/manifest.js';
import { getDataset, listDatasets, UnknownDatasetError } from '../../src/datasets/registry.js';
import customers from '../../src/datasets/customers/manifest.json' with { type: 'json' };

const clone = () => structuredClone(customers) as Record<string, any>;

describe('parseManifest', () => {
  it('accepts the customers manifest', () => {
    const m = parseManifest(customers);
    expect(m.dataset).toBe('customers');
    expect(m.columns).toHaveLength(7);
    expect(m.columns.find((c) => c.name === 'amount')?.outputName).toBe('amount_cents');
  });

  it('rejects missing fields', () => {
    const j = clone();
    delete j.dataset;
    expect(() => parseManifest(j)).toThrow(ManifestError);
    expect(() => parseManifest(j)).toThrow(/dataset/);
  });

  it('rejects an unknown column type', () => {
    const j = clone();
    j.columns[0].type = 'float';
    expect(() => parseManifest(j)).toThrow(/unknown column type/);
  });

  it('rejects an unknown normalize op', () => {
    const j = clone();
    j.columns[0].normalize = ['shout'];
    expect(() => parseManifest(j)).toThrow(/unknown normalize op/);
  });

  it('rejects a threshold outside [0,1]', () => {
    for (const t of [-0.1, 1.1]) {
      const j = clone();
      j.quarantineThreshold = t;
      expect(() => parseManifest(j)).toThrow(/quarantineThreshold/);
    }
  });

  it('rejects chunkRows < 1', () => {
    const j = clone();
    j.chunkRows = 0;
    expect(() => parseManifest(j)).toThrow(/chunkRows/);
  });

  it('rejects duplicate column names', () => {
    const j = clone();
    j.columns[1].name = 'customer_id';
    expect(() => parseManifest(j)).toThrow(/duplicate column/);
  });
});

describe('registry', () => {
  it('returns customers', () => {
    expect(getDataset('customers').manifest.schema).toBe('customers/v1');
    expect(listDatasets()).toEqual(['customers']);
  });
  it('throws for unknown datasets', () => {
    expect(() => getDataset('nope')).toThrow(UnknownDatasetError);
  });
});
