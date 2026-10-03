import { approvedFixPrefix, replayKey } from '../core/keys.js';
import type { RawRecord } from '../core/types.js';
import { getDataset } from '../datasets/registry.js';
import { NotFoundError, StateError } from '../ports.js';
import type { Deps } from './deps.js';
import { assertWorkable, readApprovedFix, type ApprovedFix } from './fix.js';
import { sha256Bytes } from './hash.js';
import { loadQuarantine } from './lineage-update.js';

export type ReplayResult =
  | { childKey: string; childSha: string; rows: number; parentRows: number[] }
  | { nothingToReplay: true; reason: string };

export function csvField(v: string | null): string {
  if (v === null) return '';
  return /[",\r\n]/.test(v) || v !== v.trim() ? `"${v.replace(/"/g, '""')}"` : v;
}

export function toCsv(header: string[], records: RawRecord[]): string {
  const lines = [header.join(',')];
  for (const r of records) lines.push(header.map((h) => csvField(r[h] ?? null)).join(','));
  return `${lines.join('\n')}\n`;
}

/**
 * Builds a replay file from the file's still-open quarantined rows (approved fixes applied) and
 * stores it under the raw bucket's `_replay/` prefix. Fixes under `fixes/proposed/` are never read.
 */
export async function replay(deps: Deps, input: { sha: string; onlyFixed?: boolean }): Promise<ReplayResult> {
  const { sha, onlyFixed = false } = input;
  const meta = await deps.control.getFile(sha);
  if (!meta) throw new NotFoundError(`no file with sha ${sha}`);
  if (meta.parentSha) throw new StateError(`file ${sha} is itself a replay of ${meta.parentSha}; replay the parent instead`);
  assertWorkable(meta, 'replay');

  const { manifest } = getDataset(meta.dataset);
  const states = await deps.control.listRowStates(sha);
  const open = (await loadQuarantine(deps, sha)).filter((q) => {
    const s = states.get(q.rowNumber) ?? 'pending';
    return s === 'pending' || s === 'requarantined';
  });

  const fixes = new Map<number, ApprovedFix>();
  for (const key of await deps.objects.list('quarantine', approvedFixPrefix(meta.dataset, sha))) {
    const m = /\/row-(\d+)\.json$/.exec(key);
    if (!m) continue;
    const f = await readApprovedFix(deps, meta.dataset, sha, Number(m[1]));
    if (f) fixes.set(f.rowNumber, f);
  }

  const header = manifest.columns.map((c) => c.name);
  const recordOf = (q: (typeof open)[number]): RawRecord => fixes.get(q.rowNumber)?.record ?? q.parsed;
  const hashOf = (r: RawRecord) => sha256Bytes(JSON.stringify(header.map((h) => r[h] ?? null)));

  // Only edges whose child file got through count as sent; a FAILED or missing child leaves its rows open.
  const sentStatuses = new Set(['LOADED', 'LOADED_WITH_QUARANTINE', 'SUPERSEDED', 'HELD']);
  const allEdges = await deps.control.listReplayEdges(sha);
  const edges = [];
  for (const e of allEdges) {
    const child = await deps.control.getFile(e.childSha);
    if (child && sentStatuses.has(child.status)) edges.push(e);
  }
  const generation = (e: { childKey: string }) => Number(/-r(\d+)\.csv$/.exec(e.childKey)?.[1] ?? 0);
  const lastSent = new Map<number, string>();
  for (const e of [...edges].sort((a, b) => generation(a) - generation(b))) {
    e.parentRows.forEach((row, i) => {
      const h = e.rowHashes?.[i];
      if (h) lastSent.set(row, h);
    });
  }

  const candidates = open.filter((q) => !onlyFixed || fixes.has(q.rowNumber));
  const chosen = candidates.filter((q) => lastSent.get(q.rowNumber) !== hashOf(recordOf(q)));
  if (chosen.length === 0) {
    if (candidates.length > 0) return { nothingToReplay: true, reason: 'open rows were already replayed unchanged' };
    return { nothingToReplay: true, reason: onlyFixed ? 'no approved fixes for open quarantined rows' : 'no open quarantined rows' };
  }

  const bytes = toCsv(header, chosen.map(recordOf));
  const childSha = sha256Bytes(bytes);
  if (edges.some((e) => e.childSha === childSha)) {
    return { nothingToReplay: true, reason: 'identical content was already replayed' };
  }

  const childKey = replayKey(meta.dataset, sha, Math.max(0, ...allEdges.map(generation)) + 1);
  const parentRows = chosen.map((q) => q.rowNumber);
  await deps.objects.put('raw', childKey, bytes);
  const rowHashes = chosen.map((q) => hashOf(recordOf(q)));
  await deps.control.putReplayEdge(sha, { childSha, childKey, parentRows, rowHashes, createdAt: deps.now().toISOString() });
  return { childKey, childSha, rows: chosen.length, parentRows };
}
