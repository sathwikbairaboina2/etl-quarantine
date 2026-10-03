import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { histogram } from '../core/histogram.js';
import { generateCustomers } from '../fixtures/generate.js';
import { fix, discard } from '../pipeline/fix.js';
import { lineageSummary, loadQuarantine } from '../pipeline/lineage-update.js';
import { readback } from '../pipeline/readback.js';
import { replay } from '../pipeline/replay.js';
import { runIngest } from '../pipeline/driver.js';
import { CliError, formatRun, ingestFile, localDeps, table } from './commands.js';

export interface DemoOptions {
  rows?: number;
  seed?: number;
  /** Must not exist or be empty. When omitted a fresh temp directory is created. */
  root?: string;
}

export interface DemoResult {
  root: string;
  sourceRows: number;
  curatedRows: number;
  stillQuarantined: number;
  discarded: number;
  loadedFromQuarantine: number;
  lost: number;
  duplicated: number;
  wallMs: number;
}

const DMY = /^(\d{2})\/(\d{2})\/(\d{4})$/;

export async function runDemo(opts: DemoOptions, out: (line: string) => void): Promise<DemoResult> {
  const rows = opts.rows ?? 50000;
  const seed = opts.seed ?? 7;
  let root: string;
  if (opts.root) {
    root = path.resolve(opts.root);
    if (fs.existsSync(root) && fs.readdirSync(root).length > 0) {
      throw new CliError(`demo root ${root} is not empty; pass a new or empty directory`);
    }
    fs.mkdirSync(root, { recursive: true });
  } else {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'etl-demo-'));
  }

  const t0 = performance.now();
  let step = 0;
  const timed = async <T>(title: string, fn: () => Promise<T>): Promise<T> => {
    const s = performance.now();
    out(`${++step}. ${title}`);
    const r = await fn();
    out(`   (${Math.round(performance.now() - s)} ms)`);
    return r;
  };

  const chunkRows = Math.max(1, Math.ceil(rows / 10));
  const deps = { ...localDeps(root), chunkRows };

  out(`etl demo: ${rows} rows, seed ${seed}, state in ${root}`);

  const file = path.join(root, 'input', 'customers.csv');
  await timed(`generate a ${rows}-row customers export with about 3% bad rows`, async () => {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const { csv, labels } = generateCustomers({ rows, badRate: 0.03, seed });
    fs.writeFileSync(file, csv!);
    out(`   ${labels.size} rows were generated bad`);
  });

  const run = await timed('ingest it, crashing chunk 2 after it wrote its output', async () => {
    const r = await ingestFile(deps, { file, dataset: 'customers', crashAfterOutput: [2], chunkRows });
    for (const line of formatRun(r).split('\n')) out(`   ${line}`);
    const attempts = r.attempts[2];
    if (attempts !== undefined) out(`   chunk 2: attempt 1 crashed, attempt ${attempts} ok`);
    return r;
  });
  const sha = run.sha;

  await timed('deliver the same bytes again as export-copy.csv', async () => {
    const r = await ingestFile(deps, { file, dataset: 'customers', keyName: 'export-copy.csv', chunkRows });
    out(`   status: ${r.status}${r.originalKey ? ` (already loaded from ${r.originalKey})` : ''}`);
  });

  const quarantined = await loadQuarantine(deps, sha);
  await timed('what is in quarantine (top 5 errors)', async () => {
    const rowsOut = [['count', 'path', 'keyword']];
    for (const h of histogram(quarantined).slice(0, 5)) rowsOut.push([String(h.count), h.instancePath || '(row)', h.keyword]);
    for (const line of table(rowsOut).split('\n')) out(`   ${line}`);
  });

  await timed('fix the DD/MM/YYYY dates, discard rows with no customer_id', async () => {
    let fixed = 0;
    let example: string | undefined;
    for (const q of quarantined) {
      const m = DMY.exec(q.parsed.contract_start ?? '');
      if (!m) continue;
      const iso = `${m[3]}-${m[2]}-${m[1]}`;
      const r = await fix(deps, { sha, rowNumber: q.rowNumber, set: { contract_start: iso } });
      if (r.stillInvalid.length === 0) {
        fixed++;
        example ??= `etl quarantine fix ${sha.slice(0, 12)} --row ${q.rowNumber} --set contract_start=${iso}`;
      }
    }
    if (example) out(`   example: ${example}`);
    out(`   applied to ${fixed} rows`);
    const noId = quarantined.filter((q) => (q.parsed.customer_id ?? '') === '').map((q) => q.rowNumber);
    if (noId.length > 0) await discard(deps, { sha, rowNumbers: noId, reason: 'missing customer_id' });
    out(`   discarded ${noId.length} rows`);
  });

  await timed('replay the fixed rows', async () => {
    const rep = await replay(deps, { sha, onlyFixed: true });
    if ('nothingToReplay' in rep) {
      out(`   nothing to replay: ${rep.reason}`);
      return;
    }
    const child = await runIngest(deps, { key: rep.childKey });
    out(`   ${rep.childKey}: ${child.status}, ${child.rowsValid} of ${child.rowsIn} rows loaded`);
    const s = await lineageSummary(deps, sha);
    out(`   parent rows: pending ${s.pending}, loaded ${s.loaded}, requarantined ${s.requarantined}, discarded ${s.discarded}`);
  });

  const rb = await readback(deps.objects, 'customers');
  const s = await lineageSummary(deps, sha);
  const stillQuarantined = s.pending + s.requarantined;
  // Quarantine files keep rows that were later loaded by a replay; count those once, as curated rows.
  const lost = rows - (rb.parquetRows + (rb.quarantineRows - s.loaded));
  const duplicated = rb.parquetRows + rb.pendingParquetRows - rb.distinctIds;
  const wallMs = performance.now() - t0;

  out(`${++step}. row accounting, read back from the Parquet and quarantine files`);
  const final = [
    ['source rows', String(rows)],
    ['curated rows', String(rb.parquetRows)],
    ['still quarantined', String(stillQuarantined)],
    ['discarded', String(s.discarded)],
    ['lost', String(lost)],
    ['duplicated', String(duplicated)],
    ['wall time', `${(wallMs / 1000).toFixed(1)} s`],
  ];
  for (const line of table(final).split('\n')) out(`   ${line}`);
  out(`lost ${lost}, duplicated ${duplicated}`);

  return {
    root,
    sourceRows: rows,
    curatedRows: rb.parquetRows,
    stillQuarantined,
    discarded: s.discarded,
    loadedFromQuarantine: s.loaded,
    lost,
    duplicated,
    wallMs,
  };
}
