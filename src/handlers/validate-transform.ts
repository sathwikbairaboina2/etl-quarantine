import type { Deps } from '../pipeline/deps.js';
import { validateTransform, type ValidateTransformInput, type ValidateTransformResult } from '../pipeline/validate-transform.js';
import { depsFromEnv, memoize } from './env.js';

export function createHandler(makeDeps: () => Deps) {
  const get = memoize(makeDeps);
  return async (event: ValidateTransformInput): Promise<ValidateTransformResult> => validateTransform(get(), event);
}

export const handler = createHandler(depsFromEnv);
