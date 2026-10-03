import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterAll, describe, expect, it } from 'vitest';
import {
  CliError,
  cmdCuratedCount,
  cmdFiles,
  cmdIngest,
  cmdPromote,
  cmdQuarantineFix,
  cmdQuarantineLs,
  cmdQuarantineShow,
  cmdReplay,
  cmdStatus,
  localDeps,
  parseSets,
} from '../../src/cli/commands.js';
import { FIXTURE_DIR, fixtureLabels } from '../support/fixtures.js';

const dirs: string[] = [];
afterAll(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});
const root = () => {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'etlq-cli-'));
  dirs.push(d);
  return d;
};
const fixture = (n: string) => path.join(FIXTURE_DIR, `${n}.csv`);
const shaOf = (out: string) => /sha: ([0-9a-f]{64})/.exec(out)![1]!;

describe('cli commands', () => {
  it('ingest, ls, show, fix, replay --only-fixed, curated count', async () => {
    const deps = localDeps(root());
    const out = await cmdIngest(deps, { file: fixture('customers-3pct'), dataset: 'customers' });
    expect(out).toContain('status: LOADED_WITH_QUARANTINE');
    expect(out).toContain('rows valid: 974');
    const sha = shaOf(out);

    const ls = await cmdQuarantineLs(deps, sha);
    expect(ls).toContain('/contract_start');
    expect(ls).toContain('total quarantined rows: 26');

    const labels = fixtureLabels('customers-3pct');
    const row = Number(Object.entries(labels.kinds).find(([, k]) => k === 'date_dmy')![0]);
    const shown = JSON.parse(await cmdQuarantineShow(deps, sha.slice(0, 10), row));
    expect(shown.rowNumber).toBe(row);
    const [dd, mm, yyyy] = (shown.parsed.contract_start as string).split('/');

    const fixed = await cmdQuarantineFix(deps, sha, row, [`contract_start=${yyyy}-${mm}-${dd}`]);
    expect(fixed).toBe(`approved row ${row}`);

    const rep = await cmdReplay(deps, sha, true);
    expect(rep).toContain(`_replay/${sha}-r1.csv`);
    expect(rep).toContain('status: LOADED');
    expect(rep).toContain('loaded 1');

    expect(await cmdReplay(deps, sha, true)).toContain('nothing to replay');

    const count = await cmdCuratedCount(deps, 'customers');
    expect(count).toContain('visible parquet rows: 975');
    expect(count).toContain('quarantine records (all files, replay children included): 26');

    expect(await cmdFiles(deps, 'customers')).toContain('LOADED_WITH_QUARANTINE');
    expect(await cmdStatus(deps, sha)).toContain('status: LOADED_WITH_QUARANTINE');
  });

  it('a HELD file is invisible until promoted', async () => {
    const deps = localDeps(root());
    const out = await cmdIngest(deps, { file: fixture('customers-20pct'), dataset: 'customers' });
    expect(out).toContain('status: HELD');
    expect(await cmdCuratedCount(deps, 'customers')).toContain('visible parquet rows: 0');
    const promoted = await cmdPromote(deps, shaOf(out));
    expect(promoted).toContain('LOADED_WITH_QUARANTINE');
    expect(await cmdCuratedCount(deps, 'customers')).toContain('visible parquet rows: 160');
  });

  it('reports a duplicate and an injected crash', async () => {
    const deps = localDeps(root());
    const first = await cmdIngest(deps, { file: fixture('customers-3pct'), dataset: 'customers', crashAfterOutput: [0], chunkRows: 400 });
    expect(first).toContain('retried chunks: 0 (2 attempts)');
    const dup = await cmdIngest(deps, { file: fixture('customers-3pct'), dataset: 'customers', keyName: 'copy.csv' });
    expect(dup).toContain('status: DUPLICATE');
    expect(dup).toContain('original: dataset=customers/customers-3pct.csv');
  });

  it('errors are one-liners', async () => {
    const deps = localDeps(root());
    await expect(cmdStatus(deps, 'deadbeef')).rejects.toThrow('no file with sha deadbeef');
    await expect(cmdIngest(deps, { file: fixture('customers-clean'), dataset: 'nope' })).rejects.toThrow(/unknown dataset/);
    await expect(cmdIngest(deps, { file: 'missing.csv', dataset: 'customers' })).rejects.toBeInstanceOf(CliError);
    expect(() => parseSets(['novalue'])).toThrow(CliError);
    expect(parseSets(['a=b=c'])).toEqual({ a: 'b=c' });
  });
});
