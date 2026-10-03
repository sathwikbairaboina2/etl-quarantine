import { isSuperseded, summarize, type LineageSummary } from '../core/lineage.js';
import type { QuarantineRecord } from '../core/types.js';
import { NotFoundError } from '../ports.js';
import type { Deps } from './deps.js';
import { readQuarantine } from './readback.js';

/** All quarantine records of one file, sorted by row number. */
export async function loadQuarantine(deps: Deps, sha: string): Promise<QuarantineRecord[]> {
  const meta = await deps.control.getFile(sha);
  if (!meta) throw new NotFoundError(`no file with sha ${sha}`);
  return readQuarantine(deps.objects, meta.dataset, sha);
}

export async function lineageSummary(deps: Deps, sha: string): Promise<LineageSummary> {
  const rows = (await loadQuarantine(deps, sha)).map((q) => q.rowNumber);
  return summarize(rows, await deps.control.listRowStates(sha));
}

/** A parent becomes SUPERSEDED once no quarantined row is pending or requarantined. */
export async function refreshParentStatus(deps: Deps, parentSha: string): Promise<LineageSummary> {
  const meta = await deps.control.getFile(parentSha);
  if (!meta) throw new NotFoundError(`no file with sha ${parentSha}`);
  const summary = await lineageSummary(deps, parentSha);
  const superseded = isSuperseded(summary);
  const updatedAt = deps.now().toISOString();
  if (superseded && meta.status === 'LOADED_WITH_QUARANTINE') {
    await deps.control.updateFile(parentSha, { status: 'SUPERSEDED', updatedAt });
  } else if (!superseded && meta.status === 'SUPERSEDED') {
    await deps.control.updateFile(parentSha, { status: 'LOADED_WITH_QUARANTINE', updatedAt });
  }
  return summary;
}

/**
 * Called when a file becomes visible. For a replay child it marks each parent row loaded or
 * requarantined from the child's own quarantine, then refreshes the parent. A no-op for other files.
 */
export async function applyReplayOutcome(deps: Deps, sha: string): Promise<void> {
  const child = await deps.control.getFile(sha);
  if (!child?.parentSha) return;
  if (child.status !== 'LOADED' && child.status !== 'LOADED_WITH_QUARANTINE') return;
  const edge = (await deps.control.listReplayEdges(child.parentSha)).find((e) => e.childSha === sha);
  if (!edge) return;
  const failedChildRows = new Set((await readQuarantine(deps.objects, child.dataset, sha)).map((q) => q.rowNumber));
  const entries = edge.parentRows.map((parentRow, i): [number, 'loaded' | 'requarantined'] => [
    parentRow,
    failedChildRows.has(i + 1) ? 'requarantined' : 'loaded',
  ]);
  await deps.control.putRowStates(child.parentSha, entries);
  await refreshParentStatus(deps, child.parentSha);
}
