import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterAll, describe, expect, it } from 'vitest';
import { FileControlStore } from '../../src/adapters/file-control-store.js';
import { FsObjectStore } from '../../src/adapters/fs-object-store.js';
import { controlStoreContract, objectStoreContract } from './contract.js';

const dirs: string[] = [];
const tmp = () => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'etlq-fs-'));
  dirs.push(d);
  return d;
};
afterAll(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

objectStoreContract('fs', () => new FsObjectStore(tmp()));
controlStoreContract('file', () => new FileControlStore(tmp()));

describe('fs specifics', () => {
  it('rejects keys that escape the bucket', async () => {
    const s = new FsObjectStore(tmp());
    await expect(s.put('raw', '../evil', 'x')).rejects.toThrow(/invalid key/);
    await expect(s.put('raw', '/abs', 'x')).rejects.toThrow(/invalid key/);
  });

  it('does not list temp files', async () => {
    const root = tmp();
    const s = new FsObjectStore(root);
    await s.put('raw', 'a/b.csv', 'x');
    fs.writeFileSync(path.join(root, 'raw', 'a', 'c.csv.tmp-1-0'), 'partial');
    expect(await s.list('raw', 'a/')).toEqual(['a/b.csv']);
  });

  it('FileControlStore reloads state written by another instance', async () => {
    const root = tmp();
    const a = new FileControlStore(root);
    await a.createFile({
      sha: 'abc',
      dataset: 'customers',
      sourceKey: 'k',
      schemaVersion: 'customers/v1',
      status: 'REGISTERED',
      columns: [],
      ingestDate: '2026-10-04',
      createdAt: '2026-10-04T09:00:00.000Z',
      updatedAt: '2026-10-04T09:00:00.000Z',
    });
    await a.putChunk('abc', { index: 2, key: 'k2', rowsIn: 5 });
    await a.putRowStates('abc', [[4, 'discarded']]);
    await a.recordDuplicate('customers', 'abc', 'copy', '2026-10-04T10:00:00.000Z');
    const b = new FileControlStore(root);
    expect((await b.getFile('abc'))?.status).toBe('REGISTERED');
    expect((await b.listChunks('abc'))[0]?.index).toBe(2);
    expect((await b.listRowStates('abc')).get(4)).toBe('discarded');
    expect(b.duplicates).toHaveLength(1);
  });
});
