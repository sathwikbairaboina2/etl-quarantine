// Packs the package, installs the tarball into a clean directory and runs the installed `etl` bin.
import { execSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'etlq-pack-'));
const q = (p) => `"${p}"`;
const run = (cmd, cwd) => execSync(cmd, { cwd, stdio: ['ignore', 'pipe', 'pipe'], encoding: 'utf8', shell: true });

try {
  run('npm run build', repo);
  const packed = run(`npm pack --pack-destination ${q(tmp)} --silent`, repo).trim().split(/\r?\n/).pop();
  const tarball = path.join(tmp, packed);
  const sizeKb = (fs.statSync(tarball).size / 1024).toFixed(0);

  const consumer = path.join(tmp, 'consumer');
  fs.mkdirSync(consumer);
  run('npm init -y', consumer);
  run(`npm install ${q(tarball)} --no-audit --no-fund`, consumer);

  const version = run('npx --no-install etl --version', consumer).trim();
  if (version !== '0.1.0') throw new Error(`expected version 0.1.0, got ${JSON.stringify(version)}`);

  const demoRoot = path.join(tmp, 'demo');
  const demo = run(`npx --no-install etl demo --rows 2000 --root ${q(demoRoot)}`, consumer);
  if (!/lost 0\b/.test(demo) || !/duplicated 0\b/.test(demo)) {
    throw new Error(`demo did not report lost 0 and duplicated 0:\n${demo}`);
  }
  console.log(`etl --version -> ${version}`);
  console.log(demo.trim().split(/\r?\n/).slice(-1)[0]);
  console.log(`PACK SMOKE OK (tarball ${packed}, ${sizeKb} KiB)`);
} catch (e) {
  console.error(`PACK SMOKE FAILED: ${e.message}`);
  if (e.stdout) console.error(e.stdout);
  if (e.stderr) console.error(e.stderr);
  process.exitCode = 1;
} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}
