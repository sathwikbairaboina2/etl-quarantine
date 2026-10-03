const pad5 = (n: number) => String(n).padStart(5, '0');

export class KeyError extends Error {
  override name = 'KeyError';
}

export const PENDING_PREFIX = '_pending/';

export function rawKey(dataset: string, file: string): string {
  return `dataset=${dataset}/${file}`;
}

export function replayKey(dataset: string, parentSha: string, n: number): string {
  return `dataset=${dataset}/_replay/${parentSha}-r${n}.csv`;
}

export function stagingChunkKey(sha: string, index: number): string {
  return `${sha}/chunk-${pad5(index)}.jsonl`;
}

export function stagingManifestKey(sha: string): string {
  return `${sha}/chunks.json`;
}

export function curatedPendingKey(dataset: string, ingestDate: string, sha: string, index: number): string {
  return `${PENDING_PREFIX}${curatedVisibleKey(dataset, ingestDate, sha, index)}`;
}

export function curatedVisibleKey(dataset: string, ingestDate: string, sha: string, index: number): string {
  return `dataset=${dataset}/ingest_date=${ingestDate}/${sha}-${pad5(index)}.parquet`;
}

export function pendingToVisible(key: string): string {
  if (!key.startsWith(PENDING_PREFIX)) throw new KeyError(`not a pending key: ${key}`);
  return key.slice(PENDING_PREFIX.length);
}

export function curatedVisiblePrefix(dataset: string): string {
  return `dataset=${dataset}/`;
}

export function curatedPendingPrefix(dataset: string): string {
  return `${PENDING_PREFIX}dataset=${dataset}/`;
}

export function quarantinePrefix(dataset: string, sha?: string): string {
  return sha ? `dataset=${dataset}/${sha}/` : `dataset=${dataset}/`;
}

export function quarantineChunkKey(dataset: string, sha: string, index: number): string {
  return `dataset=${dataset}/${sha}/chunk-${pad5(index)}.jsonl`;
}

export function approvedFixPrefix(dataset: string, sha: string): string {
  return `dataset=${dataset}/${sha}/fixes/approved/`;
}

export function approvedFixKey(dataset: string, sha: string, row: number): string {
  return `${approvedFixPrefix(dataset, sha)}row-${row}.json`;
}

export function proposedFixPrefix(dataset: string, sha: string): string {
  return `dataset=${dataset}/${sha}/fixes/proposed/`;
}

/** Parses `dataset=<name>/...`; `_replay/` keys return their dataset too. */
export function datasetFromRawKey(key: string): string {
  const m = /^dataset=([^/]+)\/.+/.exec(key);
  if (!m) throw new KeyError(`key does not start with dataset=<name>/: ${key}`);
  return m[1]!;
}

/** `dataset=x/_replay/<parentSha>-r<n>.csv` -> parentSha, otherwise undefined. */
export function parentShaFromReplayKey(key: string): string | undefined {
  const m = /^dataset=[^/]+\/_replay\/([0-9a-f]+)-r\d+\.csv$/.exec(key);
  return m?.[1];
}
