import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { NotFoundError, type BucketName, type ObjectStore } from '../ports.js';

let tmpCounter = 0;

async function renameWithRetry(from: string, to: string): Promise<void> {
  for (let attempt = 0; ; attempt++) {
    try {
      await fsp.rename(from, to);
      return;
    } catch (e) {
      const code = (e as NodeJS.ErrnoException).code;
      if (attempt < 5 && (code === 'EPERM' || code === 'EBUSY' || code === 'EACCES')) {
        await new Promise((r) => setTimeout(r, 10 * (attempt + 1)));
        continue;
      }
      throw e;
    }
  }
}

/** Maps `<root>/<bucket>/<key>`. Keys use `/` and may not escape the bucket directory. */
export class FsObjectStore implements ObjectStore {
  constructor(private readonly root: string) {}

  private file(bucket: BucketName, key: string): string {
    if (key === '' || key.startsWith('/') || key.split('/').some((s) => s === '..')) {
      throw new Error(`invalid key ${JSON.stringify(key)}`);
    }
    return path.join(this.root, bucket, ...key.split('/'));
  }

  async put(bucket: BucketName, key: string, body: Uint8Array | string): Promise<void> {
    const f = this.file(bucket, key);
    await fsp.mkdir(path.dirname(f), { recursive: true });
    const tmp = `${f}.tmp-${process.pid}-${tmpCounter++}`;
    await fsp.writeFile(tmp, body);
    await renameWithRetry(tmp, f);
  }

  async get(bucket: BucketName, key: string): Promise<Uint8Array> {
    try {
      return new Uint8Array(await fsp.readFile(this.file(bucket, key)));
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') throw new NotFoundError(`${bucket}/${key} not found`);
      throw e;
    }
  }

  async getStream(bucket: BucketName, key: string): Promise<NodeJS.ReadableStream> {
    const f = this.file(bucket, key);
    try {
      await fsp.access(f);
    } catch {
      throw new NotFoundError(`${bucket}/${key} not found`);
    }
    return fs.createReadStream(f);
  }

  async head(bucket: BucketName, key: string): Promise<{ size: number } | undefined> {
    try {
      const st = await fsp.stat(this.file(bucket, key));
      return st.isFile() ? { size: st.size } : undefined;
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
      throw e;
    }
  }

  async list(bucket: BucketName, prefix: string): Promise<string[]> {
    const base = path.join(this.root, bucket);
    const dirPart = prefix.includes('/') ? prefix.slice(0, prefix.lastIndexOf('/')) : '';
    const out: string[] = [];
    const walk = async (rel: string): Promise<void> => {
      let entries: fs.Dirent[];
      try {
        entries = await fsp.readdir(path.join(base, ...(rel ? rel.split('/') : [])), { withFileTypes: true });
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code === 'ENOENT') return;
        throw e;
      }
      for (const e of entries) {
        const r = rel ? `${rel}/${e.name}` : e.name;
        if (e.isDirectory()) {
          // only descend where the prefix can still match
          if (`${r}/`.startsWith(prefix) || prefix.startsWith(`${r}/`)) await walk(r);
        } else if (r.startsWith(prefix) && !/\.tmp-\d+-\d+$/.test(r)) {
          out.push(r);
        }
      }
    };
    await walk(dirPart);
    return out.sort();
  }

  async delete(bucket: BucketName, key: string): Promise<void> {
    await fsp.rm(this.file(bucket, key), { force: true });
  }

  async move(bucket: BucketName, fromKey: string, toKey: string): Promise<void> {
    const from = this.file(bucket, fromKey);
    const to = this.file(bucket, toKey);
    if (!(await this.head(bucket, fromKey))) {
      if (await this.head(bucket, toKey)) return;
      throw new NotFoundError(`${bucket}/${fromKey} not found`);
    }
    if (from === to) return;
    await fsp.mkdir(path.dirname(to), { recursive: true });
    await renameWithRetry(from, to);
  }
}
