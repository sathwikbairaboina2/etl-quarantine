#!/usr/bin/env node
import { Command } from 'commander';
import { VERSION } from '../version.js';
import { runDemo } from './demo.js';
import {
  cmdCuratedCount,
  cmdFiles,
  cmdIngest,
  cmdPromote,
  cmdQuarantineDiscard,
  cmdQuarantineFix,
  cmdQuarantineLs,
  cmdQuarantineShow,
  cmdReplay,
  cmdStatus,
  localDeps,
  parseIndexList,
} from './commands.js';

const program = new Command();
program
  .name('etl')
  .description('Event-driven CSV ETL with row-level quarantine, fix-and-replay and proven row conservation')
  .version(VERSION)
  .option('--root <dir>', 'local state directory', '.etl');

const deps = () => localDeps(program.opts<{ root: string }>().root);
const print = (text: string) => console.log(text);

program
  .command('ingest <file>')
  .description('copy a local CSV into the raw bucket and run the pipeline')
  .requiredOption('--dataset <name>', 'dataset name, e.g. customers')
  .option('--key-name <name>', 'object name in the raw bucket (default: file name)')
  .option('--crash-after-output <list>', 'comma list of chunk indexes that crash once after writing output')
  .action(async (file: string, o: { dataset: string; keyName?: string; crashAfterOutput?: string }) => {
    print(
      await cmdIngest(deps(), {
        file,
        dataset: o.dataset,
        ...(o.keyName ? { keyName: o.keyName } : {}),
        crashAfterOutput: parseIndexList(o.crashAfterOutput),
      }),
    );
  });

program
  .command('status <sha>')
  .description('show a file and its chunks')
  .action(async (sha: string) => print(await cmdStatus(deps(), sha)));

program
  .command('files')
  .description('list files of a dataset, newest first')
  .requiredOption('--dataset <name>')
  .action(async (o: { dataset: string }) => print(await cmdFiles(deps(), o.dataset)));

const quarantine = program.command('quarantine').description('inspect and repair quarantined rows');
quarantine
  .command('ls <sha>')
  .description('error histogram of a file')
  .action(async (sha: string) => print(await cmdQuarantineLs(deps(), sha)));
quarantine
  .command('show <sha>')
  .requiredOption('--row <n>', 'row number')
  .description('show one quarantined record')
  .action(async (sha: string, o: { row: string }) => print(await cmdQuarantineShow(deps(), sha, Number(o.row))));
quarantine
  .command('fix <sha>')
  .requiredOption('--row <n>', 'row number')
  .requiredOption('--set <col=value>', 'column assignment, repeatable', (v: string, prev: string[] = []) => [...prev, v])
  .description('approve a correction for a row')
  .action(async (sha: string, o: { row: string; set: string[] }) => print(await cmdQuarantineFix(deps(), sha, Number(o.row), o.set)));
quarantine
  .command('discard <sha>')
  .requiredOption('--rows <list>', 'comma list of row numbers')
  .option('--reason <text>')
  .description('give up on rows; they are never replayed')
  .action(async (sha: string, o: { rows: string; reason?: string }) =>
    print(await cmdQuarantineDiscard(deps(), sha, parseIndexList(o.rows), o.reason)),
  );

program
  .command('replay <sha>')
  .option('--only-fixed', 'replay only rows with an approved fix')
  .description('build a replay file from open quarantined rows and ingest it')
  .action(async (sha: string, o: { onlyFixed?: boolean }) => print(await cmdReplay(deps(), sha, Boolean(o.onlyFixed))));

program
  .command('promote <sha>')
  .description('make a HELD file visible')
  .action(async (sha: string) => print(await cmdPromote(deps(), sha)));

program
  .command('curated')
  .description('curated bucket tools')
  .command('count')
  .requiredOption('--dataset <name>')
  .description('count rows by reading Parquet and quarantine files back')
  .action(async (o: { dataset: string }) => print(await cmdCuratedCount(deps(), o.dataset)));

program
  .command('demo')
  .description('30-second tour: ingest with a crash and a duplicate delivery, quarantine, fix, replay, row accounting')
  .option('--rows <n>', 'rows to generate', '50000')
  .option('--seed <n>', 'generator seed', '7')
  .action(async (o: { rows: string; seed: string }) => {
    const explicit = program.getOptionValueSource('root') === 'cli';
    await runDemo(
      { rows: Number(o.rows), seed: Number(o.seed), ...(explicit ? { root: program.opts<{ root: string }>().root } : {}) },
      print,
    );
  });

program.parseAsync(process.argv).catch((e: unknown) => {
  const err = e as Error;
  if (process.env.ETL_DEBUG === '1') console.error(err.stack);
  else console.error(`error: ${err.message}`);
  process.exitCode = 1;
});
