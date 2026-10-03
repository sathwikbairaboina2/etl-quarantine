import type { Deps } from '../pipeline/deps.js';
import { split, type SplitResult } from '../pipeline/split.js';
import { depsFromEnv, memoize } from './env.js';

export function createHandler(makeDeps: () => Deps) {
  const get = memoize(makeDeps);
  return async (event: { sha: string }): Promise<SplitResult> => split(get(), { sha: event.sha });
}

export const handler = createHandler(depsFromEnv);
