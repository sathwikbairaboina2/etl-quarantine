import type { FileStatus } from '../ports.js';
import type { Deps, FaultHooks } from './deps.js';
import { reconcile } from './reconcile.js';
import { register } from './register.js';
import { split, type ChunkItem } from './split.js';
import { validateTransform } from './validate-transform.js';

/** Max attempts per chunk, like the state machine's Retry (maxAttempts 2 retries = 3 attempts). */
export const MAX_ATTEMPTS = 3;

export class InjectedFault extends Error {
  override name = 'InjectedFault';
}

/** A chunk index, or [chunk, times] to crash the first `times` attempts. */
export type FaultSpec = Array<number | [number, number]>;

export interface FaultPlan {
  crashAfterOutput?: FaultSpec;
  crashBeforeOutput?: FaultSpec;
  concurrency?: number;
}

export interface IngestRun {
  sha: string;
  status: FileStatus;
  rowsIn: number;
  rowsValid: number;
  rowsQuarantined: number;
  chunkCount: number;
  /** Attempts used per chunk index (1 = no retry). */
  attempts: Record<number, number>;
  durationMs: number;
  originalKey?: string;
  error?: string;
}

function specToMap(spec: FaultSpec | undefined): Map<number, number> {
  const m = new Map<number, number>();
  for (const s of spec ?? []) {
    if (typeof s === 'number') m.set(s, Math.max(m.get(s) ?? 0, 1));
    else m.set(s[0], Math.max(m.get(s[0]) ?? 0, s[1]));
  }
  return m;
}

export function faultHooks(plan: FaultPlan, inner?: FaultHooks): FaultHooks {
  const after = specToMap(plan.crashAfterOutput);
  const before = specToMap(plan.crashBeforeOutput);
  return {
    beforeOutput(chunk, attempt) {
      inner?.beforeOutput?.(chunk, attempt);
      if (attempt < (before.get(chunk) ?? 0)) throw new InjectedFault(`injected crash before output: chunk ${chunk} attempt ${attempt}`);
    },
    afterOutput(chunk, attempt) {
      inner?.afterOutput?.(chunk, attempt);
      if (attempt < (after.get(chunk) ?? 0)) throw new InjectedFault(`injected crash after output: chunk ${chunk} attempt ${attempt}`);
    },
  };
}

async function pool<T>(items: T[], concurrency: number, stopped: () => boolean, worker: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  const runners = Array.from({ length: Math.max(1, Math.min(concurrency, items.length)) }, async () => {
    while (!stopped()) {
      const i = next++;
      if (i >= items.length) return;
      await worker(items[i]!);
    }
  });
  await Promise.all(runners);
}

/** In-process mirror of the state machine: Register, Duplicate?, Split, Map(ValidateTransform, retry), Reconcile. */
export async function runIngest(base: Deps, input: { key: string }, plan: FaultPlan = {}): Promise<IngestRun> {
  const t0 = performance.now();
  const deps: Deps = plan.crashAfterOutput || plan.crashBeforeOutput ? { ...base, faults: faultHooks(plan, base.faults) } : base;
  const done = (r: Omit<IngestRun, 'durationMs'>): IngestRun => ({ ...r, durationMs: performance.now() - t0 });
  const empty = { rowsIn: 0, rowsValid: 0, rowsQuarantined: 0, chunkCount: 0, attempts: {} as Record<number, number> };

  const reg = await register(deps, input);
  if (reg.status === 'DUPLICATE') {
    const orig = await deps.control.getFile(reg.sha);
    return done({
      ...empty,
      sha: reg.sha,
      status: 'DUPLICATE',
      rowsIn: orig?.rowsIn ?? 0,
      rowsValid: orig?.rowsValid ?? 0,
      rowsQuarantined: orig?.rowsQuarantined ?? 0,
      chunkCount: orig?.chunkCount ?? 0,
      ...(reg.originalKey ? { originalKey: reg.originalKey } : {}),
    });
  }
  if (reg.status === 'FAILED') {
    return done({ ...empty, sha: reg.sha, status: 'FAILED', ...(reg.error ? { error: reg.error } : {}) });
  }

  const sha = reg.sha;
  const sp = await split(deps, { sha });
  if (sp.status === 'FAILED') {
    return done({ ...empty, sha, status: 'FAILED', ...(sp.error ? { error: sp.error } : {}) });
  }

  const items = JSON.parse(new TextDecoder().decode(await deps.objects.get('staging', sp.manifestKey))) as ChunkItem[];
  await deps.control.updateFile(sha, { status: 'PROCESSING', updatedAt: deps.now().toISOString() });

  const attempts: Record<number, number> = {};
  let failure: string | undefined;
  await pool(items, plan.concurrency ?? 4, () => failure !== undefined, async (item) => {
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      attempts[item.index] = attempt + 1;
      try {
        await validateTransform(deps, { sha, chunk: item, attempt });
        return;
      } catch (e) {
        if (attempt === MAX_ATTEMPTS - 1) {
          failure ??= `chunk ${item.index} failed after ${MAX_ATTEMPTS} attempts: ${(e as Error).message}`;
        }
      }
    }
  });

  if (failure !== undefined) {
    await deps.control.updateFile(sha, { status: 'FAILED', error: failure, updatedAt: deps.now().toISOString() });
    return done({ ...empty, sha, status: 'FAILED', rowsIn: sp.rowsIn, chunkCount: sp.chunkCount, attempts, error: failure });
  }

  const rec = await reconcile(deps, { sha });
  return done({
    sha,
    status: rec.status,
    rowsIn: rec.rowsIn,
    rowsValid: rec.rowsValid,
    rowsQuarantined: rec.rowsQuarantined,
    chunkCount: sp.chunkCount,
    attempts,
    ...(rec.error ? { error: rec.error } : {}),
  });
}
