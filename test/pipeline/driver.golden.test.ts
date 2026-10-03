import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterAll, describe, expect, it } from 'vitest';
import { FileControlStore } from '../../src/adapters/file-control-store.js';
import { FsObjectStore } from '../../src/adapters/fs-object-store.js';
import { DEFAULT_LIMITS } from '../../src/core/limits.js';
import { histogram } from '../../src/core/histogram.js';
import { runIngest } from '../../src/pipeline/driver.js';
import { conservation, readback, readQuarantine } from '../../src/pipeline/readback.js';
import type { Deps } from '../../src/pipeline/deps.js';
import { fixedClock } from '../support/clock.js';
import { FIXTURE_DIR, fixtureBytes, fixtureLabels } from '../support/fixtures.js';

const dirs: string[] = [];
afterAll(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

function localDeps(): Deps {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'etlq-golden-'));
  dirs.push(root);
  return { objects: new FsObjectStore(root), control: new FileControlStore(root), now: fixedClock, limits: { ...DEFAULT_LIMITS } };
}

const GOLDEN_DIR = path.join(FIXTURE_DIR, 'golden');
const update = process.env.UPDATE_GOLDEN === '1';

const cases: Array<[string, string]> = [
  ['customers-clean', 'LOADED'],
  ['customers-3pct', 'LOADED_WITH_QUARANTINE'],
  ['customers-20pct', 'HELD'],
];

describe.each(cases)('golden: %s', (name, expectedStatus) => {
  it('matches the golden summary and quarantine records', async () => {
    const deps = localDeps();
    const key = `dataset=customers/${name}.csv`;
    await deps.objects.put('raw', key, fixtureBytes(name));
    const run = await runIngest(deps, { key });
    expect(run.status).toBe(expectedStatus);

    const quarantine = await readQuarantine(deps.objects, 'customers');
    const summary = {
      status: run.status,
      rowsIn: run.rowsIn,
      rowsValid: run.rowsValid,
      rowsQuarantined: run.rowsQuarantined,
      histogram: histogram(quarantine),
    };
    const quarantineJsonl = quarantine.map((q) => JSON.stringify(q)).join('\n') + (quarantine.length ? '\n' : '');
    const summaryPath = path.join(GOLDEN_DIR, `${name}.json`);
    const quarPath = path.join(GOLDEN_DIR, `${name}.quarantine.jsonl`);
    if (update) {
      fs.mkdirSync(GOLDEN_DIR, { recursive: true });
      fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
      fs.writeFileSync(quarPath, quarantineJsonl);
    }
    expect(summary).toEqual(JSON.parse(fs.readFileSync(summaryPath, 'utf8')));
    expect(quarantineJsonl).toBe(fs.readFileSync(quarPath, 'utf8'));

    // The quarantined rows are exactly the generator's labelled rows.
    expect(quarantine.map((q) => q.rowNumber)).toEqual(fixtureLabels(name).badRows);

    // Readback agrees with the counters.
    const rb = await readback(deps.objects, 'customers');
    expect(rb.quarantineRows).toBe(run.rowsQuarantined);
    if (expectedStatus === 'HELD') {
      expect(rb.parquetRows).toBe(0);
      expect(rb.pendingParquetRows).toBe(run.rowsValid);
    } else {
      expect(rb.parquetRows).toBe(run.rowsValid);
      expect(rb.pendingParquetRows).toBe(0);
    }
    expect(conservation(rb, run.rowsIn)).toMatchObject({ lost: 0, duplicated: 0, conserved: true });
  });
});
