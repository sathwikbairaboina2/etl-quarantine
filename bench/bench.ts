// Headline benchmark: row conservation under injected chunk crashes and a duplicate delivery, checked by
// reading Parquet and quarantine files back (never the control table).
//
//   npm run bench                      full run (1,000,000 rows + 20-seed sweep)
//   npm run bench -- --rows 100000 --sweep-runs 3 --sweep-rows 20000 --out bench/ci
import { execSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { FileControlStore } from '../src/adapters/file-control-store.js';
import { FsObjectStore } from '../src/adapters/fs-object-store.js';
import { InMemoryControlStore, InMemoryObjectStore } from '../src/adapters/memory.js';
import { DEFAULT_LIMITS } from '../src/core/limits.js';
import { generateCustomers, mulberry32, type BadKind } from '../src/fixtures/generate.js';
import type { Deps } from '../src/pipeline/deps.js';
import { runIngest, type FaultSpec } from '../src/pipeline/driver.js';
import { conservation, readback, readQuarantine } from '../src/pipeline/readback.js';

function arg(name: string, def: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] !== undefined ? process.argv[i + 1]! : def;
}

const ROWS = Number(arg('rows', '1000000'));
const SWEEP_RUNS = Number(arg('sweep-runs', '20'));
const SWEEP_ROWS = Number(arg('sweep-rows', '100000'));
const OUT = path.resolve(arg('out', 'bench'));
const CHUNK_ROWS = 5000;
const BAD_RATE = 0.03;
const SEED = 42;
const repoRoot = path.resolve(import.meta.dirname, '..');

interface CrashPlan {
  crashAfterOutput: FaultSpec;
  crashBeforeOutput: FaultSpec;
  injected: { afterOutput: number; beforeOutput: number; doubleCrashChunks: number; total: number };
}

/** Seeded crash selection: ~5% of chunks crash after output, ~2% before, one chunk crashes twice. */
function crashPlan(chunks: number, seed: number, rates = { after: 0.05, before: 0.02 }): CrashPlan {
  const rnd = mulberry32(seed * 7919 + 13);
  const after: FaultSpec = [];
  const before: FaultSpec = [];
  let afterN = 0;
  let beforeN = 0;
  for (let c = 0; c < chunks; c++) {
    const r = rnd();
    if (r < rates.after) {
      after.push(c);
      afterN++;
    } else if (r < rates.after + rates.before) {
      before.push(c);
      beforeN++;
    }
  }
  // One chunk that crashes twice, chosen among chunks without another crash so the counts stay exact.
  const taken = new Set<number>([...after, ...before].map((x) => (typeof x === 'number' ? x : x[0])));
  const free = Array.from({ length: chunks }, (_, c) => c).filter((c) => !taken.has(c));
  let doubles = 0;
  if (free.length > 0) {
    after.push([free[Math.floor(rnd() * free.length)]!, 2]);
    afterN += 2;
    doubles = 1;
  }
  return {
    crashAfterOutput: after,
    crashBeforeOutput: before,
    injected: { afterOutput: afterN, beforeOutput: beforeN, doubleCrashChunks: doubles, total: afterN + beforeN },
  };
}

function sameRows(a: number[], b: number[]): boolean {
  return a.length === b.length && a.every((v, i) => v === b[i]);
}

function git(): string | undefined {
  try {
    return execSync('git rev-parse --short HEAD', { cwd: repoRoot, stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim();
  } catch {
    return undefined;
  }
}

const fmt = (n: number) => n.toLocaleString('en-US');

async function headline() {
  const dataDir = path.join(repoRoot, 'bench', 'data');
  fs.mkdirSync(dataDir, { recursive: true });
  const dataFile = path.join(dataDir, `customers-${ROWS}.csv`);
  console.log(`generating ${fmt(ROWS)} rows (seed ${SEED}, ${BAD_RATE * 100}% bad) -> ${dataFile}`);
  const fd = fs.openSync(dataFile, 'w');
  let buf: string[] = [];
  let size = 0;
  const { labels } = generateCustomers({
    rows: ROWS,
    badRate: BAD_RATE,
    seed: SEED,
    onLine: (line) => {
      buf.push(line, '\n');
      size += line.length + 1;
      if (size > 1 << 20) {
        fs.writeSync(fd, buf.join(''));
        buf = [];
        size = 0;
      }
    },
  });
  if (buf.length) fs.writeSync(fd, buf.join(''));
  fs.closeSync(fd);
  const bytes = fs.readFileSync(dataFile);
  const labelled = [...labels.keys()].sort((a, b) => a - b);

  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'etlq-bench-'));
  try {
    const deps: Deps = {
      objects: new FsObjectStore(root),
      control: new FileControlStore(root),
      now: () => new Date(),
      limits: { ...DEFAULT_LIMITS },
      chunkRows: CHUNK_ROWS,
    };
    const chunks = Math.ceil(ROWS / CHUNK_ROWS);
    const plan = crashPlan(chunks, SEED);
    await deps.objects.put('raw', 'dataset=customers/customers.csv', bytes);
    await deps.objects.put('raw', 'dataset=customers/customers-redelivered.csv', bytes);

    let peakRss = process.memoryUsage().rss;
    const sampler = setInterval(() => {
      peakRss = Math.max(peakRss, process.memoryUsage().rss);
    }, 100);
    console.log(`ingesting: ${chunks} chunks, ${plan.injected.total} injected crashes (${plan.injected.afterOutput} after output, ${plan.injected.beforeOutput} before output)`);
    const t0 = performance.now();
    const run = await runIngest(deps, { key: 'dataset=customers/customers.csv' }, { ...plan, concurrency: 4 });
    const ingestSeconds = (performance.now() - t0) / 1000;
    clearInterval(sampler);
    peakRss = Math.max(peakRss, process.memoryUsage().rss);

    const dup = await runIngest(deps, { key: 'dataset=customers/customers-redelivered.csv' });

    console.log('reading Parquet and quarantine files back...');
    const rb = await readback(deps.objects, 'customers');
    const c = conservation(rb, ROWS);
    const quarantined = (await readQuarantine(deps.objects, 'customers')).map((q) => q.rowNumber);
    const retries = Object.values(run.attempts).reduce((s, n) => s + (n - 1), 0);

    return {
      seed: SEED,
      badRate: BAD_RATE,
      chunkRows: CHUNK_ROWS,
      concurrency: 4,
      status: run.status,
      rowsIn: run.rowsIn,
      rowsValid: run.rowsValid,
      rowsQuarantined: run.rowsQuarantined,
      chunks: run.chunkCount,
      injectedCrashes: plan.injected,
      retries,
      duplicateDeliveryStatus: dup.status,
      readback: rb,
      sourceRows: ROWS,
      lost: c.lost,
      duplicated: c.duplicated,
      labelsMatch: sameRows(quarantined, labelled),
      ingestSeconds: Number(ingestSeconds.toFixed(2)),
      rowsPerSecond: Math.round(ROWS / ingestSeconds),
      peakRssMB: Math.round(peakRss / 1024 / 1024),
    };
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

interface SweepRun {
  seed: number;
  status: string;
  chunks: number;
  injectedCrashes: number;
  retries: number;
  duplicateDeliveryStatus: string;
  lost: number;
  duplicated: number;
  labelsMatch: boolean;
  conserved: boolean;
}

async function sweep() {
  const runs: SweepRun[] = [];
  for (let seed = 1; seed <= SWEEP_RUNS; seed++) {
    const { csv, labels } = generateCustomers({ rows: SWEEP_ROWS, badRate: BAD_RATE, seed: 1000 + seed });
    const bytes = new TextEncoder().encode(csv!);
    const deps: Deps = {
      objects: new InMemoryObjectStore(),
      control: new InMemoryControlStore(),
      now: () => new Date(),
      limits: { ...DEFAULT_LIMITS },
      chunkRows: CHUNK_ROWS,
    };
    await deps.objects.put('raw', 'dataset=customers/a.csv', bytes);
    await deps.objects.put('raw', 'dataset=customers/b.csv', bytes);
    const chunks = Math.ceil(SWEEP_ROWS / CHUNK_ROWS);
    // Heavier crash rates than the headline, always including at least one double crash.
    const plan = crashPlan(chunks, seed, { after: 0.25, before: 0.1 });
    const run = await runIngest(deps, { key: 'dataset=customers/a.csv' }, { ...plan, concurrency: 4 });
    const dup = await runIngest(deps, { key: 'dataset=customers/b.csv' });
    const rb = await readback(deps.objects, 'customers');
    const c = conservation(rb, SWEEP_ROWS);
    const quarantined = (await readQuarantine(deps.objects, 'customers')).map((q) => q.rowNumber);
    const labelsMatch = sameRows(quarantined, [...labels.keys()].sort((a, b) => a - b));
    const retries = Object.values(run.attempts).reduce((s, n) => s + (n - 1), 0);
    runs.push({
      seed,
      status: run.status,
      chunks,
      injectedCrashes: plan.injected.total,
      retries,
      duplicateDeliveryStatus: dup.status,
      lost: c.lost,
      duplicated: c.duplicated,
      labelsMatch,
      conserved: c.lost === 0 && c.duplicated === 0 && labelsMatch && run.status === 'LOADED_WITH_QUARANTINE' && dup.status === 'DUPLICATE' && retries === plan.injected.total,
    });
    console.log(`sweep ${seed}/${SWEEP_RUNS}: ${runs[runs.length - 1]!.conserved ? 'conserved' : 'NOT CONSERVED'} (${plan.injected.total} crashes)`);
  }
  return { rows: SWEEP_ROWS, store: 'in-memory object and control stores', totalRuns: runs.length, conservedRuns: runs.filter((r) => r.conserved).length, runs };
}

function table(rows: string[][]): string {
  const head = rows[0]!;
  return [`| ${head.join(' | ')} |`, `| ${head.map(() => '---').join(' | ')} |`, ...rows.slice(1).map((r) => `| ${r.join(' | ')} |`)].join('\n');
}

async function main() {
  const h = await headline();
  console.log(`headline: lost ${h.lost}, duplicated ${h.duplicated}, labelsMatch ${h.labelsMatch}, ${fmt(h.rowsPerSecond)} rows/s`);
  const s = await sweep();
  console.log(`sweep: ${s.conservedRuns}/${s.totalRuns} runs conserved every row`);

  const command = `npm run bench${process.argv.length > 2 ? ` -- ${process.argv.slice(2).join(' ')}` : ''}`;
  const results = {
    command,
    timestamp: new Date().toISOString(),
    gitCommit: git(),
    machine: {
      node: process.version,
      os: `${os.type()} ${os.release()} ${os.arch()}`,
      cpuModel: os.cpus()[0]?.model.trim(),
      cpuCount: os.cpus().length,
      totalMemoryGB: Number((os.totalmem() / 1024 ** 3).toFixed(1)),
    },
    headline: h,
    sweep: s,
  };
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, 'results.json'), `${JSON.stringify(results, null, 2)}\n`);

  const md = `# Benchmark results

Generated by \`${command}\` on ${results.timestamp}. Every number below is copied from [results.json](results.json); nothing is hand-written.

Rows are counted by **reading the Parquet and quarantine files back**, never from the control table.
\`lost = source rows - (Parquet rows + quarantine records)\`; \`duplicated = Parquet rows - distinct customer_id\`.

## Headline run

${table([
  ['Measure', 'Value'],
  ['Source rows', fmt(h.sourceRows)],
  ['Bad rows seeded', `${BAD_RATE * 100}% (seed ${h.seed})`],
  ['Chunks', `${h.chunks} of ${fmt(h.chunkRows)} rows, concurrency ${h.concurrency}`],
  ['Injected crashes', `${h.injectedCrashes.total} (${h.injectedCrashes.afterOutput} after output, ${h.injectedCrashes.beforeOutput} before output)`],
  ['Retries observed', String(h.retries)],
  ['Duplicate delivery', h.duplicateDeliveryStatus],
  ['File status', h.status],
  ['Parquet rows read back', fmt(h.readback.parquetRows)],
  ['Distinct ids in Parquet', fmt(h.readback.distinctIds)],
  ['Quarantine records read back', fmt(h.readback.quarantineRows)],
  ['**Lost rows**', `**${h.lost}**`],
  ['**Duplicated rows**', `**${h.duplicated}**`],
  ['Quarantined rows equal the generator labels', String(h.labelsMatch)],
  ['Ingest time', `${h.ingestSeconds} s`],
  ['Throughput', `${fmt(h.rowsPerSecond)} rows/s`],
  ['Peak RSS', `${h.peakRssMB} MB`],
])}

## Fault-injection sweep

${s.rows.toLocaleString('en-US')}-row files, ${s.totalRuns} seeds, ${s.store}. Each run injects random crashes (about 25% of chunks crash after output, 10% before, plus a double crash) and then re-delivers the same bytes.

**${s.conservedRuns} of ${s.totalRuns} runs conserved every row** (lost 0, duplicated 0, quarantine equals the labels, every injected crash retried, duplicate detected).

${table([
  ['Seed', 'Crashes', 'Retries', 'Status', 'Duplicate', 'Lost', 'Duplicated', 'Labels match'],
  ...s.runs.map((r) => [String(r.seed), String(r.injectedCrashes), String(r.retries), r.status, r.duplicateDeliveryStatus, String(r.lost), String(r.duplicated), String(r.labelsMatch)]),
])}

## Machine

${results.machine.cpuModel}, ${results.machine.cpuCount} logical CPUs, ${results.machine.totalMemoryGB} GB RAM, ${results.machine.os}, Node ${results.machine.node}${results.gitCommit ? `, commit ${results.gitCommit}` : ''}.

## Caveats

- One machine, local file system. No network, so this says nothing about S3 or DynamoDB latency.
- The driver stands in for Step Functions. Crashes are injected at the two points that matter (after output and before output of a chunk), not by killing processes.
- No cost number is published: nothing is deployed, so it cannot be measured.
`;
  fs.writeFileSync(path.join(OUT, 'RESULTS.md'), md);
  console.log(`wrote ${path.join(OUT, 'results.json')} and RESULTS.md`);

  const bad = h.lost !== 0 || h.duplicated !== 0 || !h.labelsMatch || s.conservedRuns !== s.totalRuns;
  if (bad) {
    console.error('NOT CONSERVED: this is a bug, not a number to publish');
    process.exitCode = 1;
  }
}

await main();
