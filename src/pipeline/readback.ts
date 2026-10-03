import { curatedPendingPrefix, curatedVisiblePrefix, quarantinePrefix } from '../core/keys.js';
import type { QuarantineRecord } from '../core/types.js';
import type { ObjectStore } from '../ports.js';
import { readParquetRows } from './parquet.js';

export interface Readback {
  /** Rows across every visible Parquet part. */
  parquetRows: number;
  /** Distinct ids across visible and pending parts. */
  distinctIds: number;
  /** One per quarantine record. */
  quarantineRows: number;
  pendingParquetRows: number;
  parquetParts: number;
}

export interface ReadbackOptions {
  /** Restrict to one file's output. */
  sha?: string;
  idColumn?: string;
}

const CHUNK_RE = /\/chunk-\d{5}\.jsonl$/;

export function quarantineChunkKeys(keys: string[]): string[] {
  return keys.filter((k) => CHUNK_RE.test(k) && !k.includes('/fixes/'));
}

function parseJsonl(bytes: Uint8Array): QuarantineRecord[] {
  return new TextDecoder()
    .decode(bytes)
    .split('\n')
    .filter((l) => l !== '')
    .map((l) => JSON.parse(l) as QuarantineRecord);
}

/** Reads quarantine records back from their JSONL files, sorted by row number. */
export async function readQuarantine(objects: ObjectStore, dataset: string, sha?: string): Promise<QuarantineRecord[]> {
  const keys = quarantineChunkKeys(await objects.list('quarantine', quarantinePrefix(dataset, sha)));
  const all: QuarantineRecord[] = [];
  for (const k of keys) all.push(...parseJsonl(await objects.get('quarantine', k)));
  return all.sort((a, b) => a.fileSha.localeCompare(b.fileSha) || a.rowNumber - b.rowNumber);
}

/** Counts rows by reading Parquet and quarantine files. Never touches the control store. */
export async function readback(objects: ObjectStore, dataset: string, opts: ReadbackOptions = {}): Promise<Readback> {
  const idColumn = opts.idColumn ?? 'customer_id';
  const inFile = (key: string) => (opts.sha ? key.includes(`/${opts.sha}-`) : true);
  const visible = (await objects.list('curated', curatedVisiblePrefix(dataset))).filter((k) => k.endsWith('.parquet') && inFile(k));
  const pending = (await objects.list('curated', curatedPendingPrefix(dataset))).filter((k) => k.endsWith('.parquet') && inFile(k));

  const ids = new Set<unknown>();
  let parquetRows = 0;
  let pendingParquetRows = 0;
  for (const k of visible) {
    const rows = await readParquetRows(await objects.get('curated', k));
    parquetRows += rows.length;
    for (const r of rows) ids.add(r[idColumn]);
  }
  for (const k of pending) {
    const rows = await readParquetRows(await objects.get('curated', k));
    pendingParquetRows += rows.length;
    for (const r of rows) ids.add(r[idColumn]);
  }

  let quarantineRows = 0;
  for (const k of quarantineChunkKeys(await objects.list('quarantine', quarantinePrefix(dataset, opts.sha)))) {
    quarantineRows += parseJsonl(await objects.get('quarantine', k)).length;
  }
  return { parquetRows, distinctIds: ids.size, quarantineRows, pendingParquetRows, parquetParts: visible.length };
}

export interface Conservation {
  sourceRows: number;
  lost: number;
  duplicated: number;
  conserved: boolean;
}

/** lost = source - (visible + pending + quarantined); duplicated = parquet rows - distinct ids. */
export function conservation(rb: Readback, sourceRows: number): Conservation {
  const present = rb.parquetRows + rb.pendingParquetRows;
  const lost = sourceRows - (present + rb.quarantineRows);
  const duplicated = present - rb.distinctIds;
  return { sourceRows, lost, duplicated, conserved: lost === 0 && duplicated === 0 };
}
