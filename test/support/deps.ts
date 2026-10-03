import { InMemoryControlStore, InMemoryObjectStore } from '../../src/adapters/memory.js';
import { DEFAULT_LIMITS } from '../../src/core/limits.js';
import type { Deps } from '../../src/pipeline/deps.js';
import { fixedClock } from './clock.js';

export type MemDeps = Deps & { objects: InMemoryObjectStore; control: InMemoryControlStore };

export function memDeps(over: Partial<Deps> = {}): MemDeps {
  return {
    objects: new InMemoryObjectStore(),
    control: new InMemoryControlStore(),
    now: fixedClock,
    limits: { ...DEFAULT_LIMITS },
    ...over,
  } as MemDeps;
}
