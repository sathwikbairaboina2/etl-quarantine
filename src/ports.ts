import type { RowState } from './core/lineage.js';

export type { RowState };

export type BucketName = 'raw' | 'staging' | 'curated' | 'quarantine';

export class NotFoundError extends Error {
  override name = 'NotFoundError';
}

/** An operation is not allowed in the current state (e.g. promote on a file that is not HELD). */
export class StateError extends Error {
  override name = 'StateError';
}

export interface ObjectStore {
  put(bucket: BucketName, key: string, body: Uint8Array | string): Promise<void>;
  get(bucket: BucketName, key: string): Promise<Uint8Array>; // throws NotFoundError
  getStream(bucket: BucketName, key: string): Promise<NodeJS.ReadableStream>;
  head(bucket: BucketName, key: string): Promise<{ size: number } | undefined>;
  list(bucket: BucketName, prefix: string): Promise<string[]>; // sorted, all pages
  delete(bucket: BucketName, key: string): Promise<void>; // no error if missing
  move(bucket: BucketName, fromKey: string, toKey: string): Promise<void>; // idempotent
}

export type FileStatus =
  | 'REGISTERED'
  | 'SPLIT'
  | 'PROCESSING'
  | 'LOADED'
  | 'LOADED_WITH_QUARANTINE'
  | 'HELD'
  | 'FAILED'
  | 'DUPLICATE'
  | 'SUPERSEDED';

export interface FileMeta {
  sha: string;
  dataset: string;
  sourceKey: string;
  schemaVersion: string;
  status: FileStatus;
  columns: string[];
  rowsIn?: number;
  rowsValid?: number;
  rowsQuarantined?: number;
  chunkCount?: number;
  ingestDate: string;
  createdAt: string;
  updatedAt: string;
  parentSha?: string;
  error?: string;
  promotedAt?: string;
}

export interface ChunkRecord {
  index: number;
  key: string;
  rowsIn: number;
  rowsValid?: number;
  rowsQuarantined?: number;
  outputKey?: string | null;
  quarantineKey?: string | null;
  attempt?: number;
}

export interface ChunkResult {
  rowsValid: number;
  rowsQuarantined: number;
  outputKey: string | null;
  quarantineKey: string | null;
  attempt: number;
}

export interface ReplayEdge {
  childSha: string;
  childKey: string;
  parentRows: number[];
  createdAt: string;
}

export interface ControlStore {
  createFile(meta: FileMeta): Promise<'created' | 'exists'>; // conditional on FILE#sha META absent
  getFile(sha: string): Promise<FileMeta | undefined>;
  updateFile(sha: string, patch: Partial<FileMeta>): Promise<void>;
  listFiles(dataset: string): Promise<FileMeta[]>; // newest first
  putChunk(sha: string, chunk: ChunkRecord): Promise<void>; // create-only: split result, rowsIn
  recordChunkResult(sha: string, index: number, r: ChunkResult): Promise<'applied' | 'stale'>; // set, never add; applies only if r.attempt >= stored attempt
  listChunks(sha: string): Promise<ChunkRecord[]>; // by index
  putReplayEdge(parentSha: string, edge: ReplayEdge): Promise<void>;
  listReplayEdges(parentSha: string): Promise<ReplayEdge[]>;
  putRowStates(parentSha: string, entries: Array<[number, RowState]>): Promise<void>; // batch
  listRowStates(parentSha: string): Promise<Map<number, RowState>>;
  recordDuplicate(dataset: string, sha: string, key: string, at: string): Promise<void>;
}
