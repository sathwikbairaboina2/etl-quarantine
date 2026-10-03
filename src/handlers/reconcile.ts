import type { Deps } from '../pipeline/deps.js';
import { reconcile, type ReconcileResult } from '../pipeline/reconcile.js';
import { depsFromEnv, memoize } from './env.js';

export function createHandler(makeDeps: () => Deps) {
  const get = memoize(makeDeps);
  return async (event: { sha: string }): Promise<ReconcileResult> => reconcile(get(), { sha: event.sha });
}

export const handler = createHandler(depsFromEnv);
