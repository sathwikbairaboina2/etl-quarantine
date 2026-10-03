import { describe, expect, it } from 'vitest';
import { summarize } from '../../src/core/lineage.js';
import { proposedFixPrefix } from '../../src/core/keys.js';
import { runIngest } from '../../src/pipeline/driver.js';
import { discard, fix } from '../../src/pipeline/fix.js';
import { loadQuarantine } from '../../src/pipeline/lineage-update.js';
import { replay } from '../../src/pipeline/replay.js';
import { StateError } from '../../src/ports.js';
import { memDeps, type MemDeps } from '../support/deps.js';

const HEADER = 'customer_id,email,country,currency,amount,contract_start,plan';
const good = (i: number) => `C-${i},a${i}@x.com,DE,EUR,1.00,2026-01-01,pro`;
const dmy = (i: number) => `C-${i},a${i}@x.com,DE,EUR,1.00,13/01/2026,pro`;

/** 400 rows; every 20th is a bad date (20 bad = exactly the 5% threshold, so the file still loads). */
const BAD_ROWS = Array.from({ length: 20 }, (_, i) => (i + 1) * 20);

async function setup(d: MemDeps) {
  const rows = Array.from({ length: 400 }, (_, i) => (BAD_ROWS.includes(i + 1) ? dmy(i + 1) : good(i + 1)));
  await d.objects.put('raw', 'dataset=customers/p.csv', `${HEADER}\n${rows.join('\n')}\n`);
  const run = await runIngest(d, { key: 'dataset=customers/p.csv' });
  expect(run.status).toBe('LOADED_WITH_QUARANTINE');
  return run.sha;
}

async function assertPartition(d: MemDeps, sha: string) {
  const q = await loadQuarantine(d, sha);
  const s = summarize(q.map((r) => r.rowNumber), await d.control.listRowStates(sha));
  expect(s.pending + s.loaded + s.requarantined + s.discarded).toBe(q.length);
  return s;
}

const iso = '2026-01-13';
const fixValid = (d: MemDeps, sha: string, row: number) => fix(d, { sha, rowNumber: row, set: { contract_start: iso } });
const runChild = async (d: MemDeps, key: string) => runIngest(d, { key });

describe('replay and lineage', () => {
  it('fixes, replays only fixed rows, and marks them loaded', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await setup(d);
    for (const r of BAD_ROWS.slice(0, 5)) expect((await fixValid(d, sha, r)).stillInvalid).toEqual([]);
    const rep = await replay(d, { sha, onlyFixed: true });
    if ('nothingToReplay' in rep) throw new Error('expected a replay');
    expect(rep.rows).toBe(5);
    expect(rep.childKey).toBe(`dataset=customers/_replay/${sha}-r1.csv`);
    const child = await runChild(d, rep.childKey);
    expect(child).toMatchObject({ status: 'LOADED', rowsIn: 5, rowsValid: 5 });
    const s = await assertPartition(d, sha);
    expect(s).toEqual({ pending: 15, loaded: 5, requarantined: 0, discarded: 0 });
    expect((await d.control.getFile(sha))?.status).toBe('LOADED_WITH_QUARANTINE');
    expect((await d.control.getFile(child.sha))?.parentSha).toBe(sha);
  });

  it('an approved fix that is still invalid is re-quarantined', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await setup(d);
    for (const r of BAD_ROWS.slice(0, 19)) await fixValid(d, sha, r);
    const bad = await fix(d, { sha, rowNumber: 400, set: { contract_start: '2026-02-30' } });
    expect(bad.stillInvalid.length).toBeGreaterThan(0);
    const rep = await replay(d, { sha, onlyFixed: true });
    if ('nothingToReplay' in rep) throw new Error('expected a replay');
    const child = await runChild(d, rep.childKey);
    expect(child).toMatchObject({ status: 'LOADED_WITH_QUARANTINE', rowsIn: 20, rowsQuarantined: 1 });
    const s = await assertPartition(d, sha);
    expect(s).toEqual({ pending: 0, loaded: 19, requarantined: 1, discarded: 0 });
    expect((await d.control.getFile(sha))?.status).toBe('LOADED_WITH_QUARANTINE');

    // The identical replay is reported, not re-ingested.
    const again = await replay(d, { sha, onlyFixed: true });
    expect(again).toMatchObject({ nothingToReplay: true });

    // Second generation: fix the last row properly; the parent becomes SUPERSEDED.
    await fixValid(d, sha, 400);
    const rep2 = await replay(d, { sha, onlyFixed: true });
    if ('nothingToReplay' in rep2) throw new Error('expected a second replay');
    expect(rep2.childKey).toBe(`dataset=customers/_replay/${sha}-r2.csv`);
    expect(rep2.parentRows).toEqual([400]);
    expect((await runChild(d, rep2.childKey)).status).toBe('LOADED');
    expect(await assertPartition(d, sha)).toEqual({ pending: 0, loaded: 20, requarantined: 0, discarded: 0 });
    expect((await d.control.getFile(sha))?.status).toBe('SUPERSEDED');
    expect(await replay(d, { sha })).toMatchObject({ nothingToReplay: true, reason: 'no open quarantined rows' });
  });

  it('ignores files under fixes/proposed/', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await setup(d);
    await d.objects.put(
      'quarantine',
      `${proposedFixPrefix('customers', sha)}row-20.json`,
      JSON.stringify({ rowNumber: 20, record: { customer_id: 'C-20', contract_start: iso } }),
    );
    expect(await replay(d, { sha, onlyFixed: true })).toMatchObject({ nothingToReplay: true });
    // A full replay carries the original bad value, not the proposal.
    const rep = await replay(d, { sha });
    if ('nothingToReplay' in rep) throw new Error('expected a replay');
    const csv = new TextDecoder().decode(await d.objects.get('raw', rep.childKey));
    expect(csv).toContain('13/01/2026');
    expect(csv.split('\n').filter(Boolean)).toHaveLength(21);
  });

  it('discarding the remaining rows supersedes the parent', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await setup(d);
    for (const r of BAD_ROWS.slice(0, 10)) await fixValid(d, sha, r);
    const rep = await replay(d, { sha, onlyFixed: true });
    if ('nothingToReplay' in rep) throw new Error('expected a replay');
    await runChild(d, rep.childKey);
    expect((await assertPartition(d, sha)).pending).toBe(10);
    await discard(d, { sha, rowNumbers: BAD_ROWS.slice(10), reason: 'unfixable' });
    expect(await assertPartition(d, sha)).toEqual({ pending: 0, loaded: 10, requarantined: 0, discarded: 10 });
    expect((await d.control.getFile(sha))?.status).toBe('SUPERSEDED');
  });

  it('discard rejects rows that are not quarantined', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await setup(d);
    await expect(discard(d, { sha, rowNumbers: [1] })).rejects.toBeInstanceOf(StateError);
    await expect(fix(d, { sha, rowNumber: 1, set: {} })).rejects.toBeInstanceOf(StateError);
  });

  it('a replay child cannot itself be replayed', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await setup(d);
    await fixValid(d, sha, 20);
    const rep = await replay(d, { sha, onlyFixed: true });
    if ('nothingToReplay' in rep) throw new Error('expected a replay');
    const child = await runChild(d, rep.childKey);
    await expect(replay(d, { sha: child.sha })).rejects.toBeInstanceOf(StateError);
  });

  it('fix merges successive edits for the same row', async () => {
    const d = memDeps({ chunkRows: 100 });
    const sha = await setup(d);
    const a = await fix(d, { sha, rowNumber: 20, set: { plan: 'gold' } });
    expect(a.stillInvalid.length).toBeGreaterThan(0);
    const b = await fix(d, { sha, rowNumber: 20, set: { contract_start: iso } });
    expect(b.stillInvalid.map((e) => e.instancePath)).toEqual(['/plan']);
    const c = await fix(d, { sha, rowNumber: 20, set: { plan: 'pro' } });
    expect(c.stillInvalid).toEqual([]);
  });
});
