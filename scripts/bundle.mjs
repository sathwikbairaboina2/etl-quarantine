// Bundles the four Lambda handlers into build/lambda/<name>/index.mjs.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const names = ['register', 'split', 'validate-transform', 'reconcile'];

fs.rmSync(path.join(repo, 'build', 'lambda'), { recursive: true, force: true });
for (const name of names) {
  const outfile = path.join(repo, 'build', 'lambda', name, 'index.mjs');
  await build({
    entryPoints: [path.join(repo, 'src', 'handlers', `${name}.ts`)],
    outfile,
    bundle: true,
    platform: 'node',
    target: 'node24',
    format: 'esm',
    sourcemap: true,
    banner: { js: "import { createRequire } from 'module'; const require = createRequire(import.meta.url);" },
    logLevel: 'warning',
  });
  const kb = (fs.statSync(outfile).size / 1024).toFixed(0);
  console.log(`bundled ${name}: ${kb} KiB`);
}
