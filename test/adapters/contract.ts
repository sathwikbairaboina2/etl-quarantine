import { randomBytes } from 'node:crypto';
import { beforeAll, describe, expect, it } from 'vitest';
import { NotFoundError, StateError, type ControlStore, type FileMeta, type ObjectStore } from '../../src/ports.js';

const uid = () => randomBytes(6).toString('hex');
const text = (b: Uint8Array) => new TextDecoder().decode(b);

async function readAll(s: NodeJS.ReadableStream): Promise<string> {
  const parts: Buffer[] = [];
  for await (const c of s) parts.push(Buffer.from(c as Uint8Array));
  return Buffer.concat(parts).toString('utf8');
}

type Maker<T> = () => T | Promise<T>;

/** Runs the shared ObjectStore behaviour checks. Every test uses a unique key prefix, so one store can be shared. */
export function objectStoreContract(name: string, makeStore: Maker<ObjectStore>): void {
  describe(`ObjectStore contract: ${name}`, () => {
    let store: ObjectStore;
    beforeAll(async () => {
      store = await makeStore();
    });

    it('puts and gets strings and bytes', async () => {
      const p = `t-${uid()}/`;
      await store.put('raw', `${p}a.txt`, 'héllo');
      await store.put('raw', `${p}b.bin`, new Uint8Array([0, 1, 2, 255]));
      expect(text(await store.get('raw', `${p}a.txt`))).toBe('héllo');
      expect([...(await store.get('raw', `${p}b.bin`))]).toEqual([0, 1, 2, 255]);
      expect(await readAll(await store.getStream('raw', `${p}a.txt`))).toBe('héllo');
    });

    it('overwrites on put', async () => {
      const k = `t-${uid()}/x`;
      await store.put('staging', k, 'one');
      await store.put('staging', k, 'two');
      expect(text(await store.get('staging', k))).toBe('two');
    });

    it('throws NotFoundError for a missing get', async () => {
      await expect(store.get('raw', `t-${uid()}/missing`)).rejects.toBeInstanceOf(NotFoundError);
    });

    it('head returns size or undefined', async () => {
      const k = `t-${uid()}/h`;
      await store.put('curated', k, 'abcde');
      expect(await store.head('curated', k)).toEqual({ size: 5 });
      expect(await store.head('curated', `${k}-nope`)).toBeUndefined();
    });

    it('lists a prefix sorted and complete across pages (1,005 keys)', async () => {
      const p = `list-${uid()}/`;
      const keys = Array.from({ length: 1005 }, (_, i) => `${p}k-${String(i).padStart(5, '0')}`);
      for (let i = 0; i < keys.length; i += 50) {
        await Promise.all(keys.slice(i, i + 50).map((k) => store.put('quarantine', k, 'x')));
      }
      await store.put('quarantine', `${p.slice(0, -1)}-other/zzz`, 'x');
      const got = await store.list('quarantine', p);
      expect(got).toEqual(keys);
    });

    it('delete of a missing key is fine', async () => {
      const k = `t-${uid()}/d`;
      await store.put('raw', k, 'x');
      await store.delete('raw', k);
      await store.delete('raw', k);
      expect(await store.head('raw', k)).toBeUndefined();
    });

    it('move moves, is idempotent, and throws when neither key exists', async () => {
      const p = `t-${uid()}/`;
      await store.put('curated', `${p}from`, 'payload');
      await store.move('curated', `${p}from`, `${p}to`);
      expect(await store.head('curated', `${p}from`)).toBeUndefined();
      expect(text(await store.get('curated', `${p}to`))).toBe('payload');
      await store.move('curated', `${p}from`, `${p}to`);
      expect(text(await store.get('curated', `${p}to`))).toBe('payload');
      await expect(store.move('curated', `${p}nope`, `${p}nope2`)).rejects.toBeInstanceOf(NotFoundError);
    });

    it('handles keys containing = and /', async () => {
      const p = `t-${uid()}`;
      const k = `_pending/dataset=customers/ingest_date=2026-10-04/${p}-00001.parquet`;
      await store.put('curated', k, 'p');
      const moved = k.replace('_pending/', '');
      await store.move('curated', k, moved);
      expect(text(await store.get('curated', moved))).toBe('p');
      expect(await store.list('curated', `dataset=customers/ingest_date=2026-10-04/${p}`)).toEqual([moved]);
    });
  });
}

function meta(sha: string, dataset: string, createdAt: string): FileMeta {
  return {
    sha,
    dataset,
    sourceKey: `dataset=${dataset}/${sha}.csv`,
    schemaVersion: `${dataset}/v1`,
    status: 'REGISTERED',
    columns: [],
    ingestDate: createdAt.slice(0, 10),
    createdAt,
    updatedAt: createdAt,
  };
}

/** Runs the shared ControlStore behaviour checks. Every test uses unique shas and dataset names. */
export function controlStoreContract(name: string, makeStore: Maker<ControlStore>): void {
  describe(`ControlStore contract: ${name}`, () => {
    let c: ControlStore;
    beforeAll(async () => {
      c = await makeStore();
    });

    it('createFile twice gives created then exists and keeps the first', async () => {
      const sha = uid();
      const m = meta(sha, `ds${uid()}`, '2026-10-04T09:00:00.000Z');
      expect(await c.createFile(m)).toBe('created');
      expect(await c.createFile({ ...m, sourceKey: 'other' })).toBe('exists');
      expect((await c.getFile(sha))?.sourceKey).toBe(m.sourceKey);
      expect(await c.getFile(uid())).toBeUndefined();
    });

    it('updateFile patches', async () => {
      const sha = uid();
      await c.createFile(meta(sha, `ds${uid()}`, '2026-10-04T09:00:00.000Z'));
      await c.updateFile(sha, { status: 'SPLIT', rowsIn: 12, columns: ['a', 'b'] });
      const f = await c.getFile(sha);
      expect(f).toMatchObject({ status: 'SPLIT', rowsIn: 12, columns: ['a', 'b'] });
      await expect(c.updateFile(uid(), { status: 'FAILED' })).rejects.toBeInstanceOf(NotFoundError);
    });

    it('updateFile with ifStatus applies only from that status', async () => {
      const sha = uid();
      await c.createFile(meta(sha, `ds${uid()}`, '2026-10-04T09:00:00.000Z'));
      await c.updateFile(sha, { status: 'FAILED', error: 'x' });
      await c.updateFile(sha, { status: 'REGISTERED', error: undefined }, { ifStatus: 'FAILED' });
      expect(await c.getFile(sha)).toMatchObject({ status: 'REGISTERED' });
      await expect(c.updateFile(sha, { status: 'SPLIT' }, { ifStatus: 'FAILED' })).rejects.toBeInstanceOf(StateError);
      expect((await c.getFile(sha))?.status).toBe('REGISTERED');
      await expect(c.updateFile(uid(), { status: 'SPLIT' }, { ifStatus: 'FAILED' })).rejects.toBeInstanceOf(NotFoundError);
    });

    it('listFiles is newest first and scoped to the dataset', async () => {
      const ds = `ds${uid()}`;
      const [a, b, d] = [uid(), uid(), uid()];
      await c.createFile(meta(a, ds, '2026-10-04T09:00:00.000Z'));
      await c.createFile(meta(b, ds, '2026-10-04T10:00:00.000Z'));
      await c.createFile(meta(d, `other${uid()}`, '2026-10-04T11:00:00.000Z'));
      expect((await c.listFiles(ds)).map((f) => f.sha)).toEqual([b, a]);
    });

    it('putChunk is create-only', async () => {
      const sha = uid();
      await c.putChunk(sha, { index: 0, key: 'k0', rowsIn: 10 });
      await c.recordChunkResult(sha, 0, { rowsValid: 9, rowsQuarantined: 1, outputKey: 'o', quarantineKey: 'q', attempt: 0 });
      await c.putChunk(sha, { index: 0, key: 'other', rowsIn: 99 });
      const [ch] = await c.listChunks(sha);
      expect(ch).toMatchObject({ index: 0, key: 'k0', rowsIn: 10, rowsValid: 9, attempt: 0 });
    });

    it('recordChunkResult applies by attempt and reports stale', async () => {
      const sha = uid();
      await c.putChunk(sha, { index: 0, key: 'k0', rowsIn: 10 });
      const r = (attempt: number, valid: number) => ({
        rowsValid: valid,
        rowsQuarantined: 10 - valid,
        outputKey: 'o',
        quarantineKey: null,
        attempt,
      });
      expect(await c.recordChunkResult(sha, 0, r(0, 8))).toBe('applied');
      expect(await c.recordChunkResult(sha, 0, r(1, 10))).toBe('applied');
      expect(await c.recordChunkResult(sha, 0, r(0, 8))).toBe('stale');
      expect(await c.recordChunkResult(sha, 0, r(1, 10))).toBe('applied');
      const [ch] = await c.listChunks(sha);
      expect(ch).toMatchObject({ rowsValid: 10, rowsQuarantined: 0, attempt: 1, outputKey: 'o' });
      expect(await c.recordChunkResult(sha, 5, r(0, 1))).toBe('stale');
    });

    it('listChunks sorts by index', async () => {
      const sha = uid();
      for (const i of [3, 0, 11, 2]) await c.putChunk(sha, { index: i, key: `k${i}`, rowsIn: 1 });
      expect((await c.listChunks(sha)).map((x) => x.index)).toEqual([0, 2, 3, 11]);
    });

    it('resetChunks removes only the named file chunks', async () => {
      const [a, b] = [uid(), uid()];
      for (const i of [0, 1, 2]) await c.putChunk(a, { index: i, key: `k${i}`, rowsIn: 1 });
      await c.putChunk(b, { index: 0, key: 'k0', rowsIn: 1 });
      await c.resetChunks(a);
      expect(await c.listChunks(a)).toEqual([]);
      expect(await c.listChunks(b)).toHaveLength(1);
      await c.putChunk(a, { index: 0, key: 'again', rowsIn: 2 });
      expect((await c.listChunks(a))[0]).toMatchObject({ key: 'again', rowsIn: 2 });
    });

    it('stores replay edges', async () => {
      const sha = uid();
      const e1 = { childSha: 'bbb', childKey: 'k1', parentRows: [1, 2], createdAt: '2026-10-04T09:00:00.000Z' };
      const e2 = { childSha: 'aaa', childKey: 'k2', parentRows: [3], createdAt: '2026-10-04T09:05:00.000Z' };
      await c.putReplayEdge(sha, e2);
      await c.putReplayEdge(sha, e1);
      const edges = await c.listReplayEdges(sha);
      expect(edges).toHaveLength(2);
      expect(edges.map((e) => e.childSha).sort()).toEqual(['aaa', 'bbb']);
      expect(edges.find((e) => e.childSha === 'bbb')?.parentRows).toEqual([1, 2]);
      expect(await c.listReplayEdges(uid())).toEqual([]);
    });

    it('stores a batch of 3,000 row states and overwrites', async () => {
      const sha = uid();
      const entries: Array<[number, 'pending' | 'loaded']> = Array.from({ length: 3000 }, (_, i) => [i + 1, 'pending']);
      await c.putRowStates(sha, entries);
      await c.putRowStates(sha, [[7, 'loaded']]);
      const m = await c.listRowStates(sha);
      expect(m.size).toBe(3000);
      expect(m.get(7)).toBe('loaded');
      expect(m.get(8)).toBe('pending');
      expect((await c.listRowStates(uid())).size).toBe(0);
    });

    it('recordDuplicate does not change META', async () => {
      const sha = uid();
      const ds = `ds${uid()}`;
      const m = meta(sha, ds, '2026-10-04T09:00:00.000Z');
      await c.createFile(m);
      const before = await c.getFile(sha);
      await c.recordDuplicate(ds, sha, 'dataset=x/copy.csv', '2026-10-04T10:00:00.000Z');
      expect(await c.getFile(sha)).toEqual(before);
      expect((await c.listFiles(ds)).map((f) => f.sha)).toEqual([sha]);
    });
  });
}
