import { checkChunk, checkFile } from '../core/accounting.js';
import { pendingToVisible } from '../core/keys.js';
import { getDataset } from '../datasets/registry.js';
import { NotFoundError, type FileStatus } from '../ports.js';
import type { Deps } from './deps.js';
import { applyReplayOutcome } from './lineage-update.js';

export interface ReconcileResult {
  status: FileStatus;
  sha: string;
  rowsIn: number;
  rowsValid: number;
  rowsQuarantined: number;
  ratio: number;
  error?: string;
}

/** Checks row conservation and either publishes the pending Parquet parts, holds them, or fails the file. */
export async function reconcile(deps: Deps, input: { sha: string }): Promise<ReconcileResult> {
  const { sha } = input;
  const meta = await deps.control.getFile(sha);
  if (!meta) throw new NotFoundError(`no file with sha ${sha}`);
  const { manifest } = getDataset(meta.dataset);
  const rowsIn = meta.rowsIn ?? 0;
  const chunks = await deps.control.listChunks(sha);

  const failed = async (error: string): Promise<ReconcileResult> => {
    await deps.control.updateFile(sha, { status: 'FAILED', error, updatedAt: deps.now().toISOString() });
    return { status: 'FAILED', sha, rowsIn, rowsValid: 0, rowsQuarantined: 0, ratio: 0, error };
  };

  if (meta.chunkCount !== undefined && chunks.length !== meta.chunkCount) {
    return failed(`expected ${meta.chunkCount} chunks, found ${chunks.length}`);
  }
  for (const c of chunks) {
    if (c.attempt === undefined || c.rowsValid === undefined || c.rowsQuarantined === undefined) {
      return failed(`chunk ${c.index} has no result`);
    }
  }
  const counts = chunks.map((c) => ({
    index: c.index,
    rowsIn: c.rowsIn,
    rowsValid: c.rowsValid ?? 0,
    rowsQuarantined: c.rowsQuarantined ?? 0,
  }));
  for (const c of counts) {
    const r = checkChunk(c);
    if (!r.ok) return failed(r.reason);
  }
  const file = checkFile(counts, rowsIn);
  if (!file.ok) return failed(file.reason);

  const rowsValid = counts.reduce((s, c) => s + c.rowsValid, 0);
  const rowsQuarantined = counts.reduce((s, c) => s + c.rowsQuarantined, 0);
  const ratio = rowsIn === 0 ? 0 : rowsQuarantined / rowsIn;
  const updatedAt = deps.now().toISOString();

  if (ratio > manifest.quarantineThreshold) {
    await deps.control.updateFile(sha, { status: 'HELD', rowsValid, rowsQuarantined, updatedAt });
    return { status: 'HELD', sha, rowsIn, rowsValid, rowsQuarantined, ratio };
  }

  await publishParts(deps, sha);
  const status: FileStatus = rowsQuarantined === 0 ? 'LOADED' : 'LOADED_WITH_QUARANTINE';
  await deps.control.updateFile(sha, { status, rowsValid, rowsQuarantined, updatedAt });
  await applyReplayOutcome(deps, sha);
  return { status, sha, rowsIn, rowsValid, rowsQuarantined, ratio };
}

/** Moves every chunk's pending Parquet part to the visible prefix. Idempotent. */
export async function publishParts(deps: Deps, sha: string): Promise<void> {
  for (const c of await deps.control.listChunks(sha)) {
    if (c.outputKey) await deps.objects.move('curated', c.outputKey, pendingToVisible(c.outputKey));
  }
}
