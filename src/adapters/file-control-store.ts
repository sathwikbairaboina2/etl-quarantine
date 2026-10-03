import fs from 'node:fs';
import path from 'node:path';
import { InMemoryControlStore, type ControlSnapshot } from './memory.js';

/** Single-writer, JSON-file-backed control store for local runs. Persists after every mutation. */
export class FileControlStore extends InMemoryControlStore {
  private readonly file: string;

  constructor(root: string) {
    super();
    this.file = path.join(root, 'control.json');
    if (fs.existsSync(this.file)) {
      this.restore(JSON.parse(fs.readFileSync(this.file, 'utf8')) as ControlSnapshot);
    }
  }

  protected override persist(): void {
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    const tmp = `${this.file}.tmp-${process.pid}`;
    fs.writeFileSync(tmp, JSON.stringify(this.snapshot()));
    fs.renameSync(tmp, this.file);
  }
}
