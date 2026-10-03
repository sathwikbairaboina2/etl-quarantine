import { describe, expect, it } from 'vitest';
import { depsFromEnv } from '../../src/handlers/env.js';
import { createHandler as createReconcile } from '../../src/handlers/reconcile.js';
import { createHandler as createRegister } from '../../src/handlers/register.js';
import { createHandler as createSplit } from '../../src/handlers/split.js';
import { createHandler as createValidate } from '../../src/handlers/validate-transform.js';
import { memDeps } from '../support/deps.js';
import { fixtureBytes } from '../support/fixtures.js';

describe('handlers', () => {
  it('run the state machine payloads end to end and return step results unchanged', async () => {
    const d = memDeps({ chunkRows: 250 });
    await d.objects.put('raw', 'dataset=customers/c.csv', fixtureBytes('customers-3pct'));

    const reg = await createRegister(() => d)({ key: 'dataset=customers/c.csv' });
    expect(reg).toMatchObject({ status: 'REGISTERED', dataset: 'customers' });

    const sp = await createSplit(() => d)({ sha: reg.sha });
    expect(sp).toMatchObject({ status: 'SPLIT', chunkCount: 4, rowsIn: 1000 });

    const items = JSON.parse(new TextDecoder().decode(await d.objects.get('staging', sp.manifestKey)));
    const validate = createValidate(() => d);
    for (const item of items) {
      // The Map state passes the item as `chunk` and the retry count as `attempt`.
      const r = await validate({ sha: reg.sha, chunk: item, attempt: 0 });
      expect(r).toMatchObject({ index: item.index, rowsIn: item.rows, attempt: 0 });
    }

    const rec = await createReconcile(() => d)({ sha: reg.sha });
    expect(rec).toMatchObject({ status: 'LOADED_WITH_QUARANTINE', rowsIn: 1000, rowsValid: 974, rowsQuarantined: 26 });
  });

  it('builds deps once per container', async () => {
    let built = 0;
    const d = memDeps();
    const h = createRegister(() => {
      built++;
      return d;
    });
    await d.objects.put('raw', 'dataset=customers/a.csv', 'x');
    await h({ key: 'dataset=customers/a.csv' });
    await h({ key: 'dataset=customers/a.csv' });
    expect(built).toBe(1);
  });
});

describe('depsFromEnv', () => {
  const full = { RAW_BUCKET: 'r', STAGING_BUCKET: 's', CURATED_BUCKET: 'c', QUARANTINE_BUCKET: 'q', CONTROL_TABLE: 't' };

  it.each(Object.keys(full))('names the missing variable %s', (name) => {
    const env = { ...full } as Record<string, string>;
    delete env[name];
    expect(() => depsFromEnv(env)).toThrow(`missing env ${name}`);
  });

  it('builds deps when everything is set and enables the drill hook only when asked', () => {
    expect(depsFromEnv(full).faults).toBeUndefined();
    const drill = depsFromEnv({ ...full, ETL_FAULT_AFTER_OUTPUT_CHUNKS: '2,5' });
    expect(() => drill.faults!.afterOutput!(2, 0)).toThrow(/drill/);
    expect(() => drill.faults!.afterOutput!(2, 1)).not.toThrow();
    expect(() => drill.faults!.afterOutput!(3, 0)).not.toThrow();
  });
});
