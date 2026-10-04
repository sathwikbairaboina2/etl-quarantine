import { approvedFixKey } from '../core/keys.js';
import { normalize } from '../core/normalize.js';
import type { RawRecord, RowError } from '../core/types.js';
import { createValidator, validateRow } from '../core/validate.js';
import { getDataset } from '../datasets/registry.js';
import { NotFoundError, StateError, type FileMeta } from '../ports.js';
import type { Deps } from './deps.js';
import { loadQuarantine, refreshParentStatus } from './lineage-update.js';

/** Rows of a file can be fixed, discarded or replayed only once the file's good rows are visible. */
export function assertWorkable(meta: FileMeta, action: string): void {
  if (meta.status === 'LOADED_WITH_QUARANTINE' || meta.status === 'SUPERSEDED') return;
  const hint = meta.status === 'HELD' ? ' (promote the file first)' : '';
  throw new StateError(`cannot ${action} ${meta.sha}: status is ${meta.status}, not LOADED_WITH_QUARANTINE or SUPERSEDED${hint}`);
}

export { loadQuarantine };

export interface ApprovedFix {
  rowNumber: number;
  record: RawRecord;
  approvedAt: string;
}

export async function readApprovedFix(deps: Deps, dataset: string, sha: string, rowNumber: number): Promise<ApprovedFix | undefined> {
  try {
    return JSON.parse(new TextDecoder().decode(await deps.objects.get('quarantine', approvedFixKey(dataset, sha, rowNumber)))) as ApprovedFix;
  } catch (e) {
    if (e instanceof NotFoundError) return undefined;
    throw e;
  }
}

/** Stores an operator-approved correction for a quarantined row. Replay validates it again. */
export async function fix(
  deps: Deps,
  input: { sha: string; rowNumber: number; set: Record<string, string> },
): Promise<{ stillInvalid: RowError[] }> {
  const { sha, rowNumber, set } = input;
  const meta = await deps.control.getFile(sha);
  if (!meta) throw new NotFoundError(`no file with sha ${sha}`);
  assertWorkable(meta, 'fix rows of');
  const q = (await loadQuarantine(deps, sha)).find((r) => r.rowNumber === rowNumber);
  if (!q) throw new StateError(`row ${rowNumber} is not quarantined in file ${sha}`);
  const state = (await deps.control.listRowStates(sha)).get(rowNumber) ?? 'pending';
  if (state !== 'pending' && state !== 'requarantined') {
    throw new StateError(`row ${rowNumber} of file ${sha} is already ${state}`);
  }

  const { manifest, schema } = getDataset(meta.dataset);
  const existing = await readApprovedFix(deps, meta.dataset, sha, rowNumber);
  const record: RawRecord = { ...(existing?.record ?? q.parsed), ...set };
  const approved: ApprovedFix = { rowNumber, record, approvedAt: deps.now().toISOString() };
  await deps.objects.put('quarantine', approvedFixKey(meta.dataset, sha, rowNumber), JSON.stringify(approved));

  const result = validateRow(createValidator(schema), normalize(record, manifest.columns));
  return { stillInvalid: result.ok ? [] : result.errors };
}

/** Marks quarantined rows as discarded; they will never be replayed. */
export async function discard(deps: Deps, input: { sha: string; rowNumbers: number[]; reason?: string }): Promise<void> {
  const { sha, rowNumbers, reason } = input;
  const meta = await deps.control.getFile(sha);
  if (!meta) throw new NotFoundError(`no file with sha ${sha}`);
  assertWorkable(meta, 'discard rows of');
  const quarantined = new Set((await loadQuarantine(deps, sha)).map((r) => r.rowNumber));
  const unknown = rowNumbers.filter((r) => !quarantined.has(r));
  if (unknown.length > 0) throw new StateError(`rows not quarantined in file ${sha}: ${unknown.join(', ')}`);
  const states = await deps.control.listRowStates(sha);
  const closed = rowNumbers.filter((r) => {
    const s = states.get(r) ?? 'pending';
    return s !== 'pending' && s !== 'requarantined';
  });
  if (closed.length > 0) throw new StateError(`rows already loaded or discarded in file ${sha}: ${closed.join(', ')}`);
  await deps.control.putRowStates(sha, rowNumbers.map((r): [number, 'discarded'] => [r, 'discarded']));
  const at = deps.now().toISOString();
  await deps.objects.put(
    'quarantine',
    `dataset=${meta.dataset}/${sha}/discarded/${at.replace(/[:.]/g, '-')}-${rowNumbers[0] ?? 0}.json`,
    JSON.stringify({ rowNumbers, reason: reason ?? null, at }),
  );
  await refreshParentStatus(deps, sha);
}
