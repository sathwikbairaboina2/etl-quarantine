import { Readable } from 'node:stream';
import {
  NotFoundError,
  StateError,
  type BucketName,
  type ChunkRecord,
  type ChunkResult,
  type ControlStore,
  type FileMeta,
  type FileStatus,
  type ObjectStore,
  type ReplayEdge,
  type RowState,
} from '../ports.js';

const enc = new TextEncoder();

export class InMemoryObjectStore implements ObjectStore {
  private readonly buckets = new Map<BucketName, Map<string, Uint8Array>>();

  private bucket(b: BucketName): Map<string, Uint8Array> {
    let m = this.buckets.get(b);
    if (!m) this.buckets.set(b, (m = new Map()));
    return m;
  }

  async put(bucket: BucketName, key: string, body: Uint8Array | string): Promise<void> {
    this.bucket(bucket).set(key, typeof body === 'string' ? enc.encode(body) : Uint8Array.from(body));
  }

  async get(bucket: BucketName, key: string): Promise<Uint8Array> {
    const v = this.bucket(bucket).get(key);
    if (!v) throw new NotFoundError(`${bucket}/${key} not found`);
    return Uint8Array.from(v);
  }

  async getStream(bucket: BucketName, key: string): Promise<NodeJS.ReadableStream> {
    return Readable.from([Buffer.from(await this.get(bucket, key))]);
  }

  async head(bucket: BucketName, key: string): Promise<{ size: number } | undefined> {
    const v = this.bucket(bucket).get(key);
    return v ? { size: v.byteLength } : undefined;
  }

  async list(bucket: BucketName, prefix: string): Promise<string[]> {
    return [...this.bucket(bucket).keys()].filter((k) => k.startsWith(prefix)).sort();
  }

  async delete(bucket: BucketName, key: string): Promise<void> {
    this.bucket(bucket).delete(key);
  }

  async move(bucket: BucketName, fromKey: string, toKey: string): Promise<void> {
    const b = this.bucket(bucket);
    const v = b.get(fromKey);
    if (!v) {
      if (b.has(toKey)) return;
      throw new NotFoundError(`${bucket}/${fromKey} not found`);
    }
    b.set(toKey, v);
    if (fromKey !== toKey) b.delete(fromKey);
  }
}

export interface DuplicateEntry {
  dataset: string;
  sha: string;
  key: string;
  at: string;
}

export interface ControlSnapshot {
  files: Record<string, FileMeta>;
  chunks: Record<string, Record<string, ChunkRecord>>;
  edges: Record<string, ReplayEdge[]>;
  rowStates: Record<string, Record<string, RowState>>;
  duplicates: DuplicateEntry[];
}

const clone = <T>(v: T): T => structuredClone(v);

export class InMemoryControlStore implements ControlStore {
  protected files = new Map<string, FileMeta>();
  protected chunks = new Map<string, Map<number, ChunkRecord>>();
  protected edges = new Map<string, ReplayEdge[]>();
  protected rowStates = new Map<string, Map<number, RowState>>();
  readonly duplicates: DuplicateEntry[] = [];

  /** Called after every mutation. File-backed subclasses persist here. */
  protected persist(): void {}

  snapshot(): ControlSnapshot {
    const obj = <V>(m: Map<string | number, V>) => Object.fromEntries(m);
    return {
      files: obj(this.files),
      chunks: Object.fromEntries([...this.chunks].map(([k, v]) => [k, obj(v)])),
      edges: obj(this.edges),
      rowStates: Object.fromEntries([...this.rowStates].map(([k, v]) => [k, obj(v)])),
      duplicates: this.duplicates,
    };
  }

  protected restore(s: ControlSnapshot): void {
    this.files = new Map(Object.entries(s.files));
    this.chunks = new Map(
      Object.entries(s.chunks).map(([k, v]) => [k, new Map(Object.entries(v).map(([i, c]) => [Number(i), c]))]),
    );
    this.edges = new Map(Object.entries(s.edges));
    this.rowStates = new Map(
      Object.entries(s.rowStates).map(([k, v]) => [k, new Map(Object.entries(v).map(([i, st]) => [Number(i), st]))]),
    );
    this.duplicates.length = 0;
    this.duplicates.push(...s.duplicates);
  }

  async createFile(meta: FileMeta): Promise<'created' | 'exists'> {
    if (this.files.has(meta.sha)) return 'exists';
    this.files.set(meta.sha, clone(meta));
    this.persist();
    return 'created';
  }

  async getFile(sha: string): Promise<FileMeta | undefined> {
    const f = this.files.get(sha);
    return f ? clone(f) : undefined;
  }

  async updateFile(sha: string, patch: Partial<FileMeta>, opts?: { ifStatus?: FileStatus }): Promise<void> {
    const f = this.files.get(sha);
    if (!f) throw new NotFoundError(`no file with sha ${sha}`);
    if (opts?.ifStatus && f.status !== opts.ifStatus) {
      throw new StateError(`file ${sha} is ${f.status}, expected ${opts.ifStatus}`);
    }
    const next = { ...f } as Record<string, unknown>;
    for (const [k, v] of Object.entries(patch)) {
      if (v === undefined) delete next[k];
      else next[k] = clone(v);
    }
    this.files.set(sha, next as unknown as FileMeta);
    this.persist();
  }

  async listFiles(dataset: string): Promise<FileMeta[]> {
    return [...this.files.values()]
      .filter((f) => f.dataset === dataset)
      .sort((a, b) => (a.createdAt === b.createdAt ? b.sha.localeCompare(a.sha) : b.createdAt.localeCompare(a.createdAt)))
      .map(clone);
  }

  async putChunk(sha: string, chunk: ChunkRecord): Promise<void> {
    let m = this.chunks.get(sha);
    if (!m) this.chunks.set(sha, (m = new Map()));
    if (m.has(chunk.index)) return;
    m.set(chunk.index, clone(chunk));
    this.persist();
  }

  async recordChunkResult(sha: string, index: number, r: ChunkResult): Promise<'applied' | 'stale'> {
    const c = this.chunks.get(sha)?.get(index);
    if (!c) return 'stale';
    if (c.attempt !== undefined && r.attempt < c.attempt) return 'stale';
    Object.assign(c, clone(r));
    this.persist();
    return 'applied';
  }

  async listChunks(sha: string): Promise<ChunkRecord[]> {
    return [...(this.chunks.get(sha)?.values() ?? [])].sort((a, b) => a.index - b.index).map(clone);
  }

  async resetChunks(sha: string): Promise<void> {
    this.chunks.delete(sha);
    this.persist();
  }

  async putReplayEdge(parentSha: string, edge: ReplayEdge): Promise<void> {
    const list = this.edges.get(parentSha) ?? [];
    const next = list.filter((e) => e.childSha !== edge.childSha);
    next.push(clone(edge));
    this.edges.set(parentSha, next);
    this.persist();
  }

  async listReplayEdges(parentSha: string): Promise<ReplayEdge[]> {
    return [...(this.edges.get(parentSha) ?? [])]
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.childSha.localeCompare(b.childSha))
      .map(clone);
  }

  async putRowStates(parentSha: string, entries: Array<[number, RowState]>): Promise<void> {
    let m = this.rowStates.get(parentSha);
    if (!m) this.rowStates.set(parentSha, (m = new Map()));
    for (const [row, st] of entries) m.set(row, st);
    this.persist();
  }

  async listRowStates(parentSha: string): Promise<Map<number, RowState>> {
    return new Map(this.rowStates.get(parentSha) ?? []);
  }

  async recordDuplicate(dataset: string, sha: string, key: string, at: string): Promise<void> {
    this.duplicates.push({ dataset, sha, key, at });
    this.persist();
  }
}
