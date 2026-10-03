import type { Limits } from '../core/types.js';
import type { ControlStore, ObjectStore } from '../ports.js';

/** Test and benchmark hooks. A hook throws to simulate a crash at that point of a chunk attempt. */
export interface FaultHooks {
  beforeOutput?(chunkIndex: number, attempt: number): void;
  afterOutput?(chunkIndex: number, attempt: number): void;
}

export interface Deps {
  objects: ObjectStore;
  control: ControlStore;
  now: () => Date;
  limits: Limits;
  faults?: FaultHooks;
  /** Overrides the manifest's chunkRows (tests, benchmark, demo). */
  chunkRows?: number;
}
