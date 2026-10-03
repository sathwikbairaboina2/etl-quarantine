import type { Deps } from './deps.js';

/**
 * Called when a file becomes visible. For a replay child this updates the parent's row states;
 * a no-op for ordinary files. (Full implementation lands with replay.)
 */
export async function applyReplayOutcome(deps: Deps, sha: string): Promise<void> {
  const meta = await deps.control.getFile(sha);
  if (!meta?.parentSha) return;
}
