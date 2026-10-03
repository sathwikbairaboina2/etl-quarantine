import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
for (const arg of process.argv.slice(2)) {
  const target = path.resolve(repo, arg);
  if (target === repo || !target.startsWith(repo + path.sep)) {
    console.error(`refusing to remove ${target}: outside the repo`);
    process.exit(1);
  }
  fs.rmSync(target, { recursive: true, force: true });
}
