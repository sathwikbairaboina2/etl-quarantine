import type { Deps } from '../pipeline/deps.js';
import { register, type RegisterResult } from '../pipeline/register.js';
import { depsFromEnv, memoize } from './env.js';

export function createHandler(makeDeps: () => Deps) {
  const get = memoize(makeDeps);
  return async (event: { key: string }): Promise<RegisterResult> => register(get(), { key: event.key });
}

export const handler = createHandler(depsFromEnv);
