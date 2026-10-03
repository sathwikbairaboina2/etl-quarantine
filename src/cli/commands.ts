import fs from 'node:fs';
import path from 'node:path';
import { FileControlStore } from '../adapters/file-control-store.js';
import { FsObjectStore } from '../adapters/fs-object-store.js';
import { histogram } from '../core/histogram.js';
import { rawKey } from '../core/keys.js';
import { DEFAULT_LIMITS } from '../core/limits.js';
import { getDataset, listDatasets } from '../datasets/registry.js';
import { NotFoundError } from '../ports.js';
import type { Deps } from '../pipeline/deps.js';
import { runIngest, type FaultPlan, type IngestRun } from '../pipeline/driver.js';
import { discard, fix } from '../pipeline/fix.js';
import { lineageSummary, loadQuarantine } from '../pipeline/lineage-update.js';
import { promote } from '../pipeline/promote.js';
import { readback } from '../pipeline/readback.js';
import { replay } from '../pipeline/replay.js';

export class CliError extends Error {
  override name = 'CliError';
}

export function localDeps(root: string, over: Partial<Deps> = {}): Deps {
  return {
    objects: new FsObjectStore(root),
    control: new FileControlStore(root),
    now: () => new Date(),
    limits: { ...DEFAULT_LIMITS },
    ...over,
  };
}

export function table(rows: string[][]): string {
  const widths = rows[0]?.map((_, c) => Math.max(...rows.map((r) => (r[c] ?? '').length))) ?? [];
  return rows.map((r) => r.map((cell, c) => cell.padEnd(widths[c] ?? 0)).join('  ').trimEnd()).join('\n');
}

/** Accepts a full sha or a unique prefix. */
export async function resolveSha(deps: Deps, input: string): Promise<string> {
  if (/^[0-9a-f]{64}$/.test(input)) {
    if (await deps.control.getFile(input)) return input;
  } else if (/^[0-9a-f]{4,63}$/.test(input)) {
    const hits: string[] = [];
    for (const ds of listDatasets()) {
      for (const f of await deps.control.listFiles(ds)) if (f.sha.startsWith(input)) hits.push(f.sha);
    }
    if (hits.length === 1) return hits[0]!;
    if (hits.length > 1) throw new CliError(`sha prefix ${input} is ambiguous (${hits.length} files)`);
  }
  throw new CliError(`no file with sha ${input}`);
}

export function parseIndexList(s: string | undefined): number[] {
  if (!s) return [];
  return s.split(',').map((x) => {
    const n = Number(x.trim());
    if (!Number.isInteger(n) || n < 0) throw new CliError(`invalid number ${JSON.stringify(x)}`);
    return n;
  });
}

export function parseSets(sets: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const s of sets) {
    const i = s.indexOf('=');
    if (i < 1) throw new CliError(`--set expects column=value, got ${JSON.stringify(s)}`);
    out[s.slice(0, i)] = s.slice(i + 1);
  }
  return out;
}

export function formatRun(run: IngestRun): string {
  const lines = [`status: ${run.status}`, `sha: ${run.sha}`];
  if (run.status === 'DUPLICATE') lines.push(`original: ${run.originalKey ?? '(unknown)'}`);
  if (run.error) lines.push(`error: ${run.error}`);
  lines.push(`rows in: ${run.rowsIn}`, `rows valid: ${run.rowsValid}`, `rows quarantined: ${run.rowsQuarantined}`);
  const retried = Object.entries(run.attempts).filter(([, n]) => n > 1);
  if (retried.length > 0) {
    lines.push(`retried chunks: ${retried.map(([c, n]) => `${c} (${n} attempts)`).join(', ')}`);
  }
  return lines.join('\n');
}

export interface IngestInput {
  file: string;
  dataset: string;
  keyName?: string;
  crashAfterOutput?: number[];
  chunkRows?: number;
}

export async function ingestFile(deps: Deps, input: IngestInput): Promise<IngestRun> {
  getDataset(input.dataset);
  if (!fs.existsSync(input.file)) throw new CliError(`file not found: ${input.file}`);
  const key = rawKey(input.dataset, input.keyName ?? path.basename(input.file));
  await deps.objects.put('raw', key, new Uint8Array(fs.readFileSync(input.file)));
  const plan: FaultPlan = input.crashAfterOutput?.length ? { crashAfterOutput: input.crashAfterOutput } : {};
  return runIngest(input.chunkRows ? { ...deps, chunkRows: input.chunkRows } : deps, { key }, plan);
}

export async function cmdIngest(deps: Deps, input: IngestInput): Promise<string> {
  return formatRun(await ingestFile(deps, input));
}

export async function cmdStatus(deps: Deps, shaInput: string): Promise<string> {
  const sha = await resolveSha(deps, shaInput);
  const meta = (await deps.control.getFile(sha))!;
  const lines = Object.entries(meta).map(([k, v]) => `${k}: ${Array.isArray(v) ? v.join(',') : String(v)}`);
  const chunks = await deps.control.listChunks(sha);
  const rows = [['chunk', 'rows in', 'valid', 'quarantined', 'attempt']];
  for (const c of chunks) {
    rows.push([String(c.index), String(c.rowsIn), String(c.rowsValid ?? '-'), String(c.rowsQuarantined ?? '-'), String(c.attempt ?? '-')]);
  }
  return `${lines.join('\n')}\n\n${table(rows)}`;
}

export async function cmdFiles(deps: Deps, dataset: string): Promise<string> {
  getDataset(dataset);
  const files = await deps.control.listFiles(dataset);
  if (files.length === 0) return `no files for dataset ${dataset}`;
  const rows = [['sha', 'status', 'rows in', 'valid', 'quarantined', 'created', 'source']];
  for (const f of files) {
    rows.push([f.sha.slice(0, 12), f.status, String(f.rowsIn ?? '-'), String(f.rowsValid ?? '-'), String(f.rowsQuarantined ?? '-'), f.createdAt, f.sourceKey]);
  }
  return table(rows);
}

export async function cmdQuarantineLs(deps: Deps, shaInput: string): Promise<string> {
  const sha = await resolveSha(deps, shaInput);
  const q = await loadQuarantine(deps, sha);
  if (q.length === 0) return 'no quarantined rows';
  const rows = [['count', 'path', 'keyword']];
  for (const h of histogram(q)) rows.push([String(h.count), h.instancePath || '(row)', h.keyword]);
  const summary = await lineageSummary(deps, sha);
  return `${table(rows)}\n\ntotal quarantined rows: ${q.length}\nstates: pending ${summary.pending}, loaded ${summary.loaded}, requarantined ${summary.requarantined}, discarded ${summary.discarded}`;
}

export async function cmdQuarantineShow(deps: Deps, shaInput: string, row: number): Promise<string> {
  const sha = await resolveSha(deps, shaInput);
  const rec = (await loadQuarantine(deps, sha)).find((r) => r.rowNumber === row);
  if (!rec) throw new CliError(`row ${row} is not quarantined in file ${sha}`);
  return JSON.stringify(rec, null, 2);
}

export async function cmdQuarantineFix(deps: Deps, shaInput: string, row: number, sets: string[]): Promise<string> {
  const sha = await resolveSha(deps, shaInput);
  const { stillInvalid } = await fix(deps, { sha, rowNumber: row, set: parseSets(sets) });
  if (stillInvalid.length === 0) return `approved row ${row}`;
  return `approved row ${row}, but it is still invalid (replay will re-quarantine it):\n${stillInvalid
    .map((e) => `  ${e.instancePath || '(row)'} ${e.message}`)
    .join('\n')}`;
}

export async function cmdQuarantineDiscard(deps: Deps, shaInput: string, rows: number[], reason?: string): Promise<string> {
  const sha = await resolveSha(deps, shaInput);
  await discard(deps, { sha, rowNumbers: rows, ...(reason ? { reason } : {}) });
  return `discarded ${rows.length} row(s)`;
}

export async function cmdReplay(deps: Deps, shaInput: string, onlyFixed: boolean): Promise<string> {
  const sha = await resolveSha(deps, shaInput);
  const rep = await replay(deps, { sha, onlyFixed });
  if ('nothingToReplay' in rep) return `nothing to replay: ${rep.reason}`;
  const child = await runIngest(deps, { key: rep.childKey });
  const s = await lineageSummary(deps, sha);
  return [
    `replay file: ${rep.childKey} (${rep.rows} rows)`,
    formatRun(child),
    `parent ${sha.slice(0, 12)}: pending ${s.pending}, loaded ${s.loaded}, requarantined ${s.requarantined}, discarded ${s.discarded}`,
  ].join('\n');
}

export async function cmdPromote(deps: Deps, shaInput: string): Promise<string> {
  const sha = await resolveSha(deps, shaInput);
  const r = await promote(deps, { sha });
  return `promoted ${sha}: ${r.status}`;
}

export async function cmdCuratedCount(deps: Deps, dataset: string): Promise<string> {
  getDataset(dataset);
  const rb = await readback(deps.objects, dataset);
  return [
    `visible parquet rows: ${rb.parquetRows}`,
    `pending parquet rows: ${rb.pendingParquetRows}`,
    `quarantine records (all files, replay children included): ${rb.quarantineRows}`,
    `distinct ids: ${rb.distinctIds}`,
    `visible parquet parts: ${rb.parquetParts}`,
  ].join('\n');
}

export { NotFoundError };
