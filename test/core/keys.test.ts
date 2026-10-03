import { describe, expect, it } from 'vitest';
import {
  approvedFixKey,
  curatedPendingKey,
  curatedVisibleKey,
  datasetFromRawKey,
  KeyError,
  parentShaFromReplayKey,
  pendingToVisible,
  proposedFixPrefix,
  quarantineChunkKey,
  rawKey,
  replayKey,
  stagingChunkKey,
  stagingManifestKey,
} from '../../src/core/keys.js';

describe('keys', () => {
  it('builds the spec layout with 5-digit padding', () => {
    expect(rawKey('customers', 'a.csv')).toBe('dataset=customers/a.csv');
    expect(replayKey('customers', 'abc', 2)).toBe('dataset=customers/_replay/abc-r2.csv');
    expect(stagingChunkKey('abc', 42)).toBe('abc/chunk-00042.jsonl');
    expect(stagingManifestKey('abc')).toBe('abc/chunks.json');
    expect(curatedPendingKey('customers', '2026-10-04', 'abc', 7)).toBe(
      '_pending/dataset=customers/ingest_date=2026-10-04/abc-00007.parquet',
    );
    expect(curatedVisibleKey('customers', '2026-10-04', 'abc', 7)).toBe(
      'dataset=customers/ingest_date=2026-10-04/abc-00007.parquet',
    );
    expect(quarantineChunkKey('customers', 'abc', 1)).toBe('dataset=customers/abc/chunk-00001.jsonl');
    expect(approvedFixKey('customers', 'abc', 12)).toBe('dataset=customers/abc/fixes/approved/row-12.json');
    expect(proposedFixPrefix('customers', 'abc')).toBe('dataset=customers/abc/fixes/proposed/');
  });

  it('pendingToVisible round-trips', () => {
    const p = curatedPendingKey('customers', '2026-10-04', 'abc', 3);
    expect(pendingToVisible(p)).toBe(curatedVisibleKey('customers', '2026-10-04', 'abc', 3));
    expect(() => pendingToVisible('dataset=x/y')).toThrow(KeyError);
  });

  it('datasetFromRawKey parses and rejects', () => {
    expect(datasetFromRawKey('dataset=customers/a.csv')).toBe('customers');
    expect(datasetFromRawKey('dataset=customers/_replay/abc-r1.csv')).toBe('customers');
    expect(() => datasetFromRawKey('other/a.csv')).toThrow(KeyError);
    expect(() => datasetFromRawKey('dataset=customers')).toThrow(KeyError);
  });

  it('parentShaFromReplayKey', () => {
    expect(parentShaFromReplayKey('dataset=customers/_replay/abc123-r2.csv')).toBe('abc123');
    expect(parentShaFromReplayKey('dataset=customers/a.csv')).toBeUndefined();
  });
});
