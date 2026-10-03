import { NotFoundError, StateError } from '../ports.js';
import type { Deps } from './deps.js';
import { applyReplayOutcome } from './lineage-update.js';
import { publishParts } from './reconcile.js';

/** Operator override for a HELD file: makes its pending parts visible. */
export async function promote(deps: Deps, input: { sha: string }): Promise<{ status: 'LOADED_WITH_QUARANTINE'; sha: string }> {
  const { sha } = input;
  const meta = await deps.control.getFile(sha);
  if (!meta) throw new NotFoundError(`no file with sha ${sha}`);
  if (meta.status !== 'HELD') throw new StateError(`cannot promote ${sha}: status is ${meta.status}, not HELD`);
  await publishParts(deps, sha);
  const at = deps.now().toISOString();
  await deps.control.updateFile(sha, { status: 'LOADED_WITH_QUARANTINE', promotedAt: at, updatedAt: at });
  await applyReplayOutcome(deps, sha);
  return { status: 'LOADED_WITH_QUARANTINE', sha };
}
