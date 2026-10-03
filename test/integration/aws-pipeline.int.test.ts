import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { histogram } from '../../src/core/histogram.js';
import { runIngest } from '../../src/pipeline/driver.js';
import { conservation, readback, readQuarantine } from '../../src/pipeline/readback.js';
import { FIXTURE_DIR, fixtureBytes, fixtureLabels } from '../support/fixtures.js';
import { awsDeps } from './support.js';

const KEY = 'dataset=customers/customers-3pct.csv';
const golden = JSON.parse(fs.readFileSync(path.join(FIXTURE_DIR, 'golden', 'customers-3pct.json'), 'utf8'));

describe.skipIf(!process.env.ETL_IT)('pipeline on the AWS adapters', () => {
  it('the 3% fixture gives the golden counts and histogram', async () => {
    const d = await awsDeps({ chunkRows: 100 });
    await d.objects.put('raw', KEY, fixtureBytes('customers-3pct'));
    const run = await runIngest(d, { key: KEY });
    const q = await readQuarantine(d.objects, 'customers');
    expect({ status: run.status, rowsIn: run.rowsIn, rowsValid: run.rowsValid, rowsQuarantined: run.rowsQuarantined, histogram: histogram(q) }).toEqual(golden);
    expect(q.map((r) => r.rowNumber)).toEqual(fixtureLabels('customers-3pct').badRows);
    const rb = await readback(d.objects, 'customers');
    expect(conservation(rb, run.rowsIn)).toMatchObject({ lost: 0, duplicated: 0, conserved: true });
    expect(run.chunkCount).toBe(10);
  });

  it('a duplicate delivery is detected by the conditional write', async () => {
    const d = await awsDeps({ chunkRows: 100 });
    await d.objects.put('raw', KEY, fixtureBytes('customers-3pct'));
    const first = await runIngest(d, { key: KEY });
    const before = await readback(d.objects, 'customers');
    await d.objects.put('raw', 'dataset=customers/copy.csv', fixtureBytes('customers-3pct'));
    const second = await runIngest(d, { key: 'dataset=customers/copy.csv' });
    expect(second).toMatchObject({ status: 'DUPLICATE', sha: first.sha, originalKey: KEY });
    expect(await readback(d.objects, 'customers')).toEqual(before);
    expect((await d.control.getFile(first.sha))?.status).toBe('LOADED_WITH_QUARANTINE');
  });

  it('a chunk that crashes after writing output still conserves rows', async () => {
    const d = await awsDeps({ chunkRows: 100 });
    await d.objects.put('raw', KEY, fixtureBytes('customers-3pct'));
    const run = await runIngest(d, { key: KEY }, { crashAfterOutput: [3, [6, 2]], crashBeforeOutput: [1] });
    expect(run.status).toBe('LOADED_WITH_QUARANTINE');
    expect(run.attempts[3]).toBe(2);
    expect(run.attempts[6]).toBe(3);
    const rb = await readback(d.objects, 'customers');
    expect(rb.parquetRows).toBe(run.rowsValid);
    expect(conservation(rb, run.rowsIn)).toMatchObject({ lost: 0, duplicated: 0 });
  });
});
