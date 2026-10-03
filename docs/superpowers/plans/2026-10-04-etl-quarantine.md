# etl-quarantine v0.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task by task. Use superpowers:test-driven-development inside every task: write the listed tests first, run them and see them fail, then implement. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship v0.1 of an event-driven CSV ETL with row-level quarantine, fix-and-replay, and proven row conservation. It has a pure TypeScript core, local and AWS adapters, an in-process driver that mirrors the Step Functions state machine, an `etl` CLI with a 30-second demo, CDK stacks proven by assertion tests and `cdk synth`, and a measured 1M-row fault-injection benchmark.

**Architecture:** One ESM npm package (`etl-quarantine`, bin `etl`). `src/core/` is pure (no AWS SDK, no fs). `src/ports.ts` defines `ObjectStore` and `ControlStore`. `src/adapters/` holds in-memory, filesystem/JSON-file and S3/DynamoDB adapters. `src/pipeline/` holds the steps (register, split, validateTransform, reconcile) and operator actions (fix, discard, replay, promote), plus `driver.ts`. `src/handlers/` wraps the steps as Lambdas. `infra/` is the CDK app.

**Spec:** `docs/superpowers/specs/2026-10-04-etl-quarantine.md`. **Read it first**: it fixes key layouts, statuses, the dataset columns, the port interfaces and the benchmark protocol. Decisions are in `docs/adr/0001`-`0007`. The original design is `C:\Users\sathwik\projects\taskarinchu\docs\devdocs\etl-quarantine.md` (read-only, outside this repo).

**Tech stack (exact pins, all checked on npm on 2026-10-04):**
- Runtime deps: `ajv@8.20.0`, `ajv-formats@3.0.1`, `csv-parse@7.0.3`, `hyparquet@1.31.1`, `hyparquet-writer@0.16.10`, `commander@15.0.0`, `@aws-sdk/client-s3@3.1146.0`, `@aws-sdk/client-dynamodb@3.1146.0`, `@aws-sdk/lib-dynamodb@3.1146.0`.
- Dev deps: `typescript@5.9.3` (not 7.x), `vitest@4.1.11` (not 5.x), `fast-check@4.10.2`, `tsx@4.23.15`, `esbuild@0.28.2`, `@types/node@24.19.1`, `@types/aws-lambda@8.10.164`, `aws-cdk-lib@2.272.0`, `aws-cdk@2.1144.0`, `constructs@10.8.1`, `cdk-nag@3.0.2`.
- Emulators (integration only): `amazon/dynamodb-local:3.3.1`, `adobe/s3mock:4.11.0`.

**Status at plan time:** The repo has `git init -b main`, the spec, ADRs, this plan and the ledger. No code exists. Every task is to do.

## Prototype results the builder can rely on (2026-10-04, scratch, not in repo)

- `hyparquet-writer` `parquetWriteBuffer({ columnData, schema })` with an explicit schema writes `INT32`+`DATE` from `Date` values, `INT64` from `BigInt`, `BYTE_ARRAY`+`UTF8` from strings; `hyparquet` `parquetReadObjects({ file: arrayBuffer })` reads them back (`Date`, `bigint`, `string`). A `null` in a `REQUIRED` column throws.
- `csv-parse@7` stream with `{ bom: true, raw: true, info: true, relax_column_count: true, skip_empty_lines: true }` handles BOM, CRLF, quoted commas and quoted newlines; each record has `.record`, `.raw` (raw text incl. trailing `\r`), `.info.records`.
- Ajv under TS NodeNext needs the cast trick (otherwise `new Ajv2020()` does not typecheck):
  ```ts
  import _Ajv2020 from 'ajv/dist/2020.js';
  import _addFormats from 'ajv-formats';
  const Ajv2020 = _Ajv2020 as unknown as typeof _Ajv2020.default;
  const addFormats = _addFormats as unknown as typeof _addFormats.default;
  ```
  `format: "date"` rejects `2026-02-30` and `2026-13-01`.
- DynamoDB Local 3.3.1: `PutCommand` with `ConditionExpression: 'attribute_not_exists(pk)'` throws `ConditionalCheckFailedException` on the second put; an attempt-guarded `UpdateCommand` throws it for a stale attempt.
- S3Mock 4.11.0 (path style, any credentials): `CopyObject`, `DeleteObject` of a missing key (no error), `HeadObject` missing gives `NotFound` (404), `GetObject` missing gives `NoSuchKey` (404), `ListObjectsV2` pages at 1000 with `NextContinuationToken`.
- CDK 2.272.0: `sfn.DistributedMap` with `itemReader: new sfn.S3JsonItemReader({ bucket, key: sfn.JsonPath.stringAt('$.split.manifestKey') })` synthesizes `ItemReader` + `DISTRIBUTED`. `sfn.JsonPath.numberAt('$$.State.RetryCount')` inside `TaskInput.fromObject` renders `"attempt.$":"$$.State.RetryCount"`. `events.Match.wildcard('dataset=*.csv')` and `events.Match.greaterThan(0)` render `{"wildcard":...}` and `{"numeric":[">",0]}`.

## Global constraints

- Repo root: `C:\Users\sathwik\projects\taskarinchu\etl-quarantine`. Edit nothing outside it.
- Never deploy to AWS or LocalStack. No AWS credentials are needed for anything except the integration suite, which uses dummy credentials against the emulators.
- No global installs. Use `npx` only for binaries from devDependencies.
- Host ports 5340-5349 only. Containers must be named `etl-quarantine-*` and be stopped/removed after use (`npm run it:down`).
- `npm test` must pass with no network, no Docker and no credentials. Integration files are `test/integration/*.int.test.ts` and use `describe.skipIf(!process.env.ETL_IT)`.
- ESM everywhere (`"type": "module"`, `module: NodeNext`). **Every relative import ends in `.js`**, even in `.ts` files. JSON imports use `with { type: 'json' }`.
- `src/core/**` must not import `@aws-sdk/*`, `node:fs`, `node:fs/promises`, or anything under `src/adapters/`. Task 2 adds a test that enforces this.
- All `package.json` scripts must work on Windows and Linux: no inline `VAR=x cmd`, no `rm -rf`, no `&&` inside node scripts that assume bash. Use small `scripts/*.mjs` helpers.
- Numbers in README/DEVDOCS come only from `bench/results.json` or recorded command output. Never invent or round up a number.
- Commits: one small conventional commit per task, with the subject given in the task. Every message ends with a blank line and `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Commits are authorized **locally only**. Never push, never add a remote, never amend or squash. If a permission check blocks a commit, stop and report it; do not work around it.
- Never commit `.env*` (except `.env.example`), `node_modules/`, `dist/`, `build/`, `cdk.out/`, `.etl/`, `bench/data/`, `*.tgz`.
- Keep context small: pipe noisy output, e.g. `npm test 2>&1 | tail -30`.
- After each task append one line to the ledger `.superpowers/sdd/2026-10-04-etl-quarantine/progress.md`: `Task N: complete (<real test counts / outputs>) | commit: "<subject>"`. Any deviation from this plan gets its own line: `Ruling: <what> - <why> - <cost>`.

## Review focus (inputs a happy-path test would miss; each has a test in its owning task)

1. **CSV edge cases:** BOM, CRLF, a quoted field with a comma, a quoted field with a newline, a trailing blank line, a ragged row (too many and too few columns), an unclosed quote at EOF (Tasks 5, 9).
2. **Crash windows:** a chunk that crashes after writing output but before recording counts; a crash before writing; two crashes on the same chunk; a stale attempt arriving after a newer one (Tasks 4, 11, 14).
3. **Duplicate delivery:** the same bytes under a second key, and the same key delivered twice (Tasks 8, 14).
4. **Threshold boundary:** a quarantine ratio exactly equal to the threshold is not `HELD`; one row more is (Task 12).
5. **Replay honesty:** an approved fix that is still invalid is re-quarantined, a file under `fixes/proposed/` is ignored, a replay with nothing new is reported and not re-ingested, a child replay file cannot itself be replayed (Task 15).
6. **Readback, not counters:** the benchmark and the golden tests count rows by reading Parquet and quarantine files, and a test proves the checker notices a deleted Parquet part (Tasks 14, 23).

---

## File structure

```
etl-quarantine/
  package.json  package-lock.json  tsconfig.json  tsconfig.build.json  vitest.config.ts  cdk.json
  .gitignore  .env.example  LICENSE  README.md  docker-compose.yml
  .github/workflows/ci.yml
  scripts/clean.mjs  scripts/bundle.mjs  scripts/pack-smoke.mjs  scripts/it.mjs  scripts/gen-fixture.ts
  src/
    ports.ts                      # ObjectStore, ControlStore, records, statuses, errors (spec "Interfaces")
    core/
      types.ts                    # Manifest, ColumnSpec, QuarantineRecord, RowError, ValidRow brand, Limits
      manifest.ts                 # parseManifest (validates manifest shape)
      csv.ts                      # checkHeader, fieldsToRecord, column-count errors
      normalize.ts                # normalize ops
      validate.ts                 # Ajv compile/cache, validateRow -> ValidRow | RowError[]
      cast.ts                     # ValidRow -> typed Parquet values, InvariantError
      accounting.ts               # chunk/file conservation checks
      keys.ts                     # every S3 key builder
      limits.ts                   # DEFAULT_LIMITS, checkFileSize, checkColumns
      lineage.ts                  # row-state partition and SUPERSEDED rule
      histogram.ts                # error histogram for quarantine ls
    datasets/
      registry.ts                 # static imports of manifests + schemas
      customers/manifest.json
      customers/v1.schema.json
    adapters/
      memory.ts                   # InMemoryObjectStore, InMemoryControlStore
      fs-object-store.ts          # FsObjectStore
      file-control-store.ts       # FileControlStore (extends the in-memory store, persists control.json)
      s3-object-store.ts          # S3ObjectStore
      dynamo-control-store.ts     # DynamoControlStore
    pipeline/
      deps.ts                     # Deps type, FaultHooks
      hash.ts                     # sha256 of a stream
      register.ts  split.ts  parquet.ts  validate-transform.ts  reconcile.ts
      fix.ts  replay.ts  promote.ts  lineage-update.ts
      driver.ts                   # runIngest(deps, key, faultPlan) mirrors the state machine
      readback.ts                 # counts rows from Parquet + quarantine files (used by CLI, tests, bench)
    handlers/
      env.ts  register.ts  split.ts  validate-transform.ts  reconcile.ts
    cli/
      main.ts                     # #!/usr/bin/env node, commander program
      commands.ts                 # command implementations (testable, return text)
      demo.ts
    fixtures/generate.ts          # seeded generator (used by scripts, demo, bench)
  infra/
    bin/app.ts  lib/storage-stack.ts  lib/pipeline-stack.ts  lib/nag-suppressions.ts
    test/storage.test.ts  test/pipeline.test.ts  test/nag.test.ts
  test/
    core/*.test.ts  adapters/*.test.ts  adapters/contract.ts  pipeline/*.test.ts  cli/*.test.ts
    integration/aws-adapters.int.test.ts  integration/aws-pipeline.int.test.ts
    fixtures/customers-clean.csv  customers-3pct.csv  customers-20pct.csv  *.labels.json
    fixtures/golden/*.json  fixtures/golden/*.quarantine.jsonl
    support/clock.ts  support/deps.ts
  bench/bench.ts  bench/results.json  bench/RESULTS.md
```

---

## Task 1: Scaffold the package

**Files:** `package.json`, `tsconfig.json`, `tsconfig.build.json`, `vitest.config.ts`, `.gitignore`, `.env.example`, `LICENSE`, `scripts/clean.mjs`, `test/smoke.test.ts`, `src/version.ts`.

- [ ] `package.json`: `"name": "etl-quarantine"`, `"version": "0.1.0"`, `"type": "module"`, `"license": "MIT"`, `"engines": { "node": ">=22.12" }`, `"bin": { "etl": "dist/cli/main.js" }`, `"files": ["dist", "README.md", "LICENSE"]`. Exact (no caret) versions from the tech stack. Scripts:
  - `"typecheck": "tsc -p tsconfig.json --noEmit"`
  - `"test": "vitest run"`
  - `"build": "node scripts/clean.mjs dist && tsc -p tsconfig.build.json"`
  - (later tasks add `bundle`, `synth`, `fixtures`, `demo`, `bench`, `smoke:pack`, `it:up`, `it:down`, `test:it`, `gates`)
- [ ] `tsconfig.json`: `module`/`moduleResolution` `NodeNext`, `target` `ES2023`, `strict`, `noUncheckedIndexedAccess`, `resolveJsonModule`, `esModuleInterop`, `skipLibCheck: true`, `types: ["node"]`, include `src`, `test`, `infra`, `scripts`, `bench`. `tsconfig.build.json` extends it with `rootDir: "src"`, `outDir: "dist"`, `declaration: true`, `sourceMap: true`, include `src` only.
- [ ] `vitest.config.ts`: `include: ['test/**/*.test.ts', 'infra/test/**/*.test.ts']`, `testTimeout: 60000`, `pool: 'forks'`.
- [ ] `.gitignore`: `node_modules/ dist/ build/ cdk.out/ .etl/ bench/data/ coverage/ *.tgz .env .env.* !.env.example`.
- [ ] `.env.example`: comment-only; `ETL_IT=` and the two emulator endpoints (`ETL_IT_DDB_ENDPOINT=http://127.0.0.1:5340`, `ETL_IT_S3_ENDPOINT=http://127.0.0.1:5341`). No secrets.
- [ ] `LICENSE`: MIT, `Copyright (c) 2026 sathwikbairaboina2`.
- [ ] `scripts/clean.mjs`: `fs.rmSync(arg, { recursive: true, force: true })` for each argv path, refusing paths outside the repo.
- [ ] `src/version.ts` exports `VERSION = '0.1.0'`. `test/smoke.test.ts` asserts it equals `package.json` version (read with `fs`).
- [ ] Run `npm install` (this creates `package-lock.json`), then `npm run typecheck` and `npm test`.
  - Expected: typecheck exit 0; `1 passed`.
- [ ] Commit: `chore: scaffold etl-quarantine package`.

## Task 2: Core types, manifest parsing, dataset registry, purity guard

**Files:** `src/core/types.ts`, `src/core/manifest.ts`, `src/datasets/customers/manifest.json`, `src/datasets/customers/v1.schema.json`, `src/datasets/registry.ts`, `test/core/manifest.test.ts`, `test/core/purity.test.ts`.

- [ ] Types (`src/core/types.ts`): `ColumnType = 'string' | 'int64_cents' | 'date'`; `ColumnSpec { name; type; required: boolean; normalize: NormalizeOp[]; outputName?: string }` (`amount` has `outputName: "amount_cents"`); `Manifest { dataset; schema: string /* "customers/v1" */; format: 'csv'; header: true; columns: ColumnSpec[]; partitionBy: 'ingest_date'; quarantineThreshold: number; chunkRows: number }`; `RowError { instancePath; keyword; message }`; `QuarantineRecord { fileSha; chunk; rowNumber; raw; parsed: Record<string,string|null>; schemaVersion; errors: RowError[]; quarantinedAt }`; `declare const validBrand: unique symbol; type ValidRow = Readonly<Record<string, string|null>> & { readonly [validBrand]: true }`.
- [ ] `customers/manifest.json` and `v1.schema.json` exactly as the spec's dataset table (`$schema` 2020-12, `$id` `"customers/v1"`, `additionalProperties: false`, email `type: ["string","null"]`, all others `type: "string"` and required).
- [ ] `parseManifest(json: unknown): Manifest` throws `ManifestError` with a clear message on: missing fields, unknown column type, unknown normalize op, threshold outside `[0,1]`, `chunkRows < 1`, duplicate column names.
- [ ] `registry.ts`: `getDataset(name) -> { manifest, schema }`, throws `UnknownDatasetError` for unknown names; `listDatasets()`.
- [ ] Tests: manifest happy path for `customers`; one failing case per rule above (6 cases); registry unknown dataset; `purity.test.ts` walks `src/core/**/*.ts` and fails if any file matches `/from ['"](@aws-sdk\/|node:fs|\.\.\/adapters)/`.
- [ ] Run `npx vitest run test/core` - expected all pass (~10 tests). Commit: `feat(core): add manifest types, customers dataset and purity guard`.

## Task 3: Normalisers, validation, cast

**Files:** `src/core/normalize.ts`, `src/core/validate.ts`, `src/core/cast.ts`, `test/core/normalize.test.ts`, `test/core/validate.test.ts`, `test/core/cast.test.ts`.

- [ ] `normalize(record, columns)` applies ops in listed order; `emptyToNull` turns `''` into `null`; other ops skip `null`.
- [ ] `createValidator(schema)` compiles once per `$id` (module-level `Map`), Ajv options `{ allErrors: true, strict: true, coerceTypes: false }` plus `addFormats`. `validateRow(validator, record) -> { ok: true, row: ValidRow } | { ok: false, errors: RowError[] }`.
- [ ] `cast(row: ValidRow, columns) -> Record<string, string|bigint|Date|null>`: `int64_cents` parses `^-?\d+(\.\d{1,2})?$` into exact cents with string arithmetic (no floats: `"1.5"` -> `150n`, `"-0.05"` -> `-5n`); `date` -> `new Date(Date.UTC(y, m-1, d))`. Anything unexpected throws `InvariantError`.
- [ ] Tests (I2 table, at least 12 rows in `validate.test.ts`): missing required field; `customer_id` `"X-1"`; email `"foo@"`; country `"DEU"`; currency `"EURO"`; amount `"1,50"`; amount `"1.505"`; contract_start `"13/01/2026"`; `"2026-02-30"`; plan `"gold"`; extra property; two errors in one row reported together (allErrors). Valid rows: email `null`; messy-but-valid `"  A@B.COM "` after normalise. Cast tests: cents edge cases (`"0"`, `"12"`, `"12.3"`, `"-7.05"`, `"999999999999.99"`), date conversion, and `InvariantError` for a hand-forged bad `ValidRow` (cast via `as unknown as ValidRow`).
- [ ] Run `npx vitest run test/core` - all pass. Commit: `feat(core): normalise, validate with Ajv 2020-12 and cast valid rows`.

## Task 4: Accounting, limits, keys, lineage, histogram (pure)

**Files:** `src/core/accounting.ts`, `src/core/limits.ts`, `src/core/keys.ts`, `src/core/lineage.ts`, `src/core/histogram.ts`, tests `test/core/accounting.property.test.ts`, `test/core/limits.test.ts`, `test/core/keys.test.ts`, `test/core/lineage.property.test.ts`, `test/core/histogram.test.ts`.

- [ ] `checkChunk({ rowsIn, rowsValid, rowsQuarantined })` and `checkFile(chunks, fileRowsIn)` return `{ ok: true } | { ok: false, reason }` (reason names the chunk and the three numbers).
- [ ] `DEFAULT_LIMITS = { maxFileBytes: 5 * 1024 ** 3, maxRowBytes: 1024 ** 2, maxColumns: 200 }`; `checkFileSize(size, limits)`, `checkColumns(n, limits)` throw `LimitError` with the limit and actual value in the message.
- [ ] `keys.ts` builds every key from the spec ("Differences from the design doc"), with chunk numbers padded to 5 digits: `rawKey`, `replayKey(dataset, parentSha, n)`, `stagingChunkKey`, `stagingManifestKey`, `curatedPendingKey`, `curatedVisibleKey`, `pendingToVisible(key)`, `quarantineChunkKey`, `approvedFixKey(dataset, sha, row)`, `proposedFixPrefix`, `datasetFromRawKey(key)` (parses `dataset=<name>/`, throws on anything else; `_replay/` keys return the dataset too).
- [ ] `lineage.ts`: `RowState = 'pending' | 'loaded' | 'requarantined' | 'discarded'`; `summarize(quarantinedRows: number[], states: Map<number, RowState>) -> counts per state` (rows missing from `states` count as pending); `isSuperseded(summary)` true when `pending + requarantined === 0` and there is at least one quarantined row.
- [ ] `histogram(records) -> Array<{ instancePath, keyword, count }>` sorted by count desc, then path.
- [ ] Property tests (fast-check, `numRuns: 500`): (a) for random arrays of chunk counts where `valid + quarantined = in`, `checkFile` is ok; changing any one count by ±1 makes it fail. (b) Lineage: for a random set of quarantined row numbers and random state assignment, the per-state counts sum to the number of quarantined rows (each row in exactly one state), and `isSuperseded` matches its definition.
- [ ] Limits boundary tests at limit and limit+1 for all three limits (I8). Keys tests: round-trip `pendingToVisible`, padding, `datasetFromRawKey` errors.
- [ ] Run `npx vitest run test/core` - all pass. Commit: `feat(core): add row accounting, limits, key layout and lineage rules`.

## Task 5: CSV record helpers (pure)

**Files:** `src/core/csv.ts`, `test/core/csv.test.ts`.

- [ ] `checkHeader(headerFields, manifest)` returns the column order; throws `HeaderError` listing missing and unexpected columns (order may differ from the manifest; matching is exact, case-sensitive, after trimming and BOM removal).
- [ ] `fieldsToRecord(fields, header) -> { record, errors }`: when `fields.length !== header.length`, `errors = [{ instancePath: '', keyword: 'columnCount', message: 'expected N fields, got M' }]` and `record` maps the fields that exist (missing ones are `null`).
- [ ] Tests: header reordered OK; missing column; extra column; ragged short/long rows.
- [ ] Commit: `feat(core): map CSV fields to records with column-count errors`.

## Task 6: Ports and the in-memory adapters + contract suite

**Files:** `src/ports.ts`, `src/adapters/memory.ts`, `test/adapters/contract.ts`, `test/adapters/memory.test.ts`.

- [ ] `src/ports.ts` exactly as the spec's "Interfaces", plus: `FileMeta { sha, dataset, sourceKey, schemaVersion, status, columns: string[], rowsIn?, rowsValid?, rowsQuarantined?, chunkCount?, ingestDate, createdAt, updatedAt, parentSha?, error?, promotedAt? }`; `ChunkRecord { index, key, rowsIn, rowsValid?, rowsQuarantined?, outputKey?: string|null, quarantineKey?: string|null, attempt? }`; `ChunkResult` (the result fields + `attempt`); `ReplayEdge { childSha, childKey, parentRows: number[], createdAt }`; `putRowStates(parentSha, entries: Array<[number, RowState]>)` **replaces** `putRowState` (batch form); `recordDuplicate(dataset, sha, key, at)`; `NotFoundError`.
  - `putChunk` is create-only: if the chunk already exists it is left unchanged (so a re-run of split never clobbers results).
  - `recordChunkResult` applies only if the chunk exists and `r.attempt >= stored attempt` (or none stored); returns `'stale'` otherwise.
- [ ] `test/adapters/contract.ts` exports `objectStoreContract(name, makeStore)` and `controlStoreContract(name, makeStore)`; each registers a `describe` block. Cover: put/get bytes and strings; get missing throws `NotFoundError`; head size; list prefix sorted and complete with 1,005 keys (pagination); delete missing is fine; move moves, move again is a no-op, move with neither key throws `NotFoundError`; keys containing `=` and `/`. Control: createFile twice -> `created`, `exists`; updateFile patches; listFiles newest first; putChunk create-only; recordChunkResult attempt 0 applied, attempt 1 applied, stale attempt 0 after 1 -> `stale`, same attempt again -> applied with identical values; listChunks sorted by index; edges; row states batch of 3,000 entries; recordDuplicate does not change META.
- [ ] `memory.ts` implements both; `memory.test.ts` runs both contracts.
- [ ] Run `npx vitest run test/adapters` - all pass. Commit: `feat(adapters): define ports and in-memory stores with a shared contract suite`.

## Task 7: Filesystem object store and JSON-file control store

**Files:** `src/adapters/fs-object-store.ts`, `src/adapters/file-control-store.ts`, `test/adapters/fs.test.ts`.

- [ ] `FsObjectStore(root)` maps to `<root>/<bucket>/<key>`; keys use `/` and must not contain `..` or start with `/` (throw). `put` writes to `<file>.tmp-<pid>` then renames. `list` walks recursively and returns POSIX-style keys, sorted. `getStream` returns `fs.createReadStream`.
- [ ] `FileControlStore(root)` extends `InMemoryControlStore`, loads `<root>/control.json` if present, and persists after every mutation with temp-file + rename. Persist `Map`s as plain objects.
- [ ] `fs.test.ts` runs both contracts on a fresh `fs.mkdtempSync(os.tmpdir())` per test, plus a reload test (write with one instance, read with a new one).
- [ ] Commit: `feat(adapters): add filesystem object store and JSON-file control store`.

## Task 8: Hashing and `register`

**Files:** `src/pipeline/deps.ts`, `src/pipeline/hash.ts`, `src/pipeline/register.ts`, `test/support/clock.ts`, `test/support/deps.ts`, `test/pipeline/register.test.ts`.

- [ ] `Deps { objects: ObjectStore; control: ControlStore; now: () => Date; limits: Limits; faults?: FaultHooks }`. `test/support/deps.ts` builds in-memory deps with a fixed clock (`2026-10-04T09:00:00Z`, `test/support/clock.ts`).
- [ ] `sha256Stream(stream) -> hex`.
- [ ] `register(deps, { key }) -> { status: 'REGISTERED' | 'DUPLICATE' | 'FAILED', sha, dataset, key, originalKey?, error? }`:
  1. `head('raw', key)`; missing -> throw `NotFoundError`. Size over `maxFileBytes` -> return `FAILED` with the `LimitError` message (no META is written; the file is not hashed).
  2. Resolve dataset from the key; unknown dataset -> `FAILED`.
  3. Hash the stream. `createFile({ status: 'REGISTERED', ingestDate: now().toISOString().slice(0,10), createdAt, schemaVersion, sourceKey: key, parentSha? })`. `parentSha` comes from a replay key (`_replay/<parentSha>-r<n>.csv`).
  4. `exists` -> `recordDuplicate(...)` and return `DUPLICATE` with `originalKey` from the existing META. An existing META in any status (including `FAILED`) is a duplicate (Ruling in spec: no automatic re-run).
- [ ] Tests: first registration; same bytes under a second key -> `DUPLICATE` with `originalKey`; same key twice -> `DUPLICATE`; size at limit OK and limit+1 `FAILED` (override `limits.maxFileBytes` to 10); unknown dataset; replay key sets `parentSha`; META `ingestDate` comes from the clock.
- [ ] Commit: `feat(pipeline): register files by content hash with duplicate detection`.

## Task 9: `split`

**Files:** `src/pipeline/split.ts`, `test/pipeline/split.test.ts`.

- [ ] `split(deps, { sha }) -> { status: 'SPLIT' | 'FAILED', sha, chunkCount, rowsIn, manifestKey, error? }`. Stream the raw object through `csv-parse` with `{ bom: true, raw: true, info: true, relax_column_count: true, skip_empty_lines: true, max_record_size: limits.maxRowBytes }`. The first record is the header: `checkColumns`, then `checkHeader`. Each data record becomes a staging line `{"rowNumber":n,"raw":"<raw without trailing \r\n>","fields":[...]}`. Flush a chunk every `manifest.chunkRows` rows to `stagingChunkKey(sha, i)`, then `putChunk` with `rowsIn`.
- [ ] After the last chunk: write `stagingManifestKey(sha)` (a JSON array `[{ sha, index, key, rows }]`, the Distributed Map item list), then `updateFile(sha, { status: 'SPLIT', columns: header, rowsIn, chunkCount })`.
- [ ] Parse errors, `LimitError` and `HeaderError` -> `updateFile(status: 'FAILED', error)` and return `FAILED`. Anything else throws (the state machine retries).
- [ ] A file with a header and zero rows -> `SPLIT` with `chunkCount 0` and an empty item list.
- [ ] Tests: 12 rows with `chunkRows` 5 -> 3 chunks of 5/5/2 and correct row numbers; BOM + CRLF + quoted comma + quoted newline row keeps its raw text; trailing blank line not counted; ragged row passes through to staging (it is quarantined later); unclosed quote at EOF -> `FAILED` with the csv-parse message and no chunks visible; row over `maxRowBytes` (override to 64) -> `FAILED`; 201 columns -> `FAILED`; header mismatch -> `FAILED`; zero-row file.
- [ ] Commit: `feat(pipeline): stream-split CSV files into staged chunks`.

## Task 10: Parquet writer and reader helpers

**Files:** `src/pipeline/parquet.ts`, `test/pipeline/parquet.test.ts`.

- [ ] `writeParquet(rows: ValidRow[], columns: ColumnSpec[]) -> Uint8Array`. Cast each row (Task 3), build `columnData` per output column and an explicit `schema`: root `{ name: 'root', num_children: n }`; `string` -> `{ type: 'BYTE_ARRAY', converted_type: 'UTF8' }`; `int64_cents` -> `{ type: 'INT64' }`; `date` -> `{ type: 'INT32', converted_type: 'DATE' }`; `repetition_type` `REQUIRED` or `OPTIONAL` from `required`. The function signature only accepts `ValidRow[]`.
- [ ] `readParquetRows(bytes) -> Promise<Record<string, unknown>[]>` via `hyparquet` `parquetReadObjects({ file: arrayBuffer })` (convert the `Uint8Array` to a tight `ArrayBuffer` slice).
- [ ] Tests: round-trip 3 rows including a `null` email, negative cents and a date; 5,000 rows round-trip with exact count; output column names use `outputName` (`amount_cents`).
- [ ] Commit: `feat(pipeline): write curated Parquet parts with explicit schema`.

## Task 11: `validateTransform`

**Files:** `src/pipeline/validate-transform.ts`, `test/pipeline/validate-transform.test.ts`.

- [ ] `validateTransform(deps, { sha, chunk: { index, key, rows }, attempt }) -> ChunkResult`:
  1. Load META and the manifest; read the staging chunk.
  2. Per line: `fieldsToRecord` (column-count errors), `normalize`, `validateRow`. Collect `ValidRow[]` and `QuarantineRecord[]` (`quarantinedAt = now()`, `parsed` = the normalised record, `schemaVersion` from META).
  3. `faults?.beforeOutput?.(index, attempt)`.
  4. Write the Parquet part to `curatedPendingKey(dataset, META.ingestDate, sha, index)` if there is at least one valid row, and the quarantine JSONL to `quarantineChunkKey` if there is at least one quarantined row. Use `META.ingestDate`, **never** `now()`, so retries hit the same key.
  5. `checkChunk` against `chunk.rows`; a mismatch throws `InvariantError`.
  6. `faults?.afterOutput?.(index, attempt)`.
  7. `recordChunkResult(sha, index, { rowsValid, rowsQuarantined, outputKey, quarantineKey, attempt })`.
- [ ] Tests: a chunk with 7 good and 3 bad rows -> Parquet contains exactly the 7 `customer_id`s (read back) and none of the bad ones (I2); quarantine JSONL has 3 records with the right `rowNumber`, `raw` and Ajv errors; a chunk with only bad rows writes no Parquet (`outputKey: null`); running the same chunk twice (attempt 0 then 1) leaves one Parquet object with the same bytes and the counts set (not doubled); a stale attempt result is ignored; an `afterOutput` hook that throws leaves output written but no counts recorded.
- [ ] Commit: `feat(pipeline): validate and transform chunks into pending Parquet and quarantine`.

## Task 12: `reconcile` and `promote`

**Files:** `src/pipeline/reconcile.ts`, `src/pipeline/promote.ts`, `src/pipeline/lineage-update.ts` (stub for now: `applyReplayOutcome` no-op when `parentSha` is unset), `test/pipeline/reconcile.test.ts`.

- [ ] `reconcile(deps, { sha }) -> { status, rowsIn, rowsValid, rowsQuarantined, ratio }`:
  1. `listChunks`; a chunk without `attempt` -> `FAILED` (`chunk N has no result`).
  2. `checkChunk` each and `checkFile` against META `rowsIn` -> `FAILED` on mismatch. Nothing is moved.
  3. `ratio = rowsQuarantined / rowsIn` (0 for an empty file). `ratio > manifest.quarantineThreshold` -> `HELD` (parts stay pending).
  4. Otherwise move each chunk's `outputKey` to `pendingToVisible(outputKey)`, then status `LOADED` (no quarantine) or `LOADED_WITH_QUARANTINE`, and call `applyReplayOutcome(deps, sha)`.
  5. `updateFile` with status, counts and `updatedAt`. Reconcile is idempotent: running it twice gives the same state (moves are idempotent).
- [ ] `promote(deps, { sha })`: only for `HELD` (else throw `StateError`); move parts; status `LOADED_WITH_QUARANTINE`, `promotedAt`; `applyReplayOutcome`.
- [ ] Tests (I1, I6): clean file -> `LOADED`, parts visible, nothing under `_pending/`; ratio exactly equal to the threshold (e.g. 1 of 20 with threshold 0.05) -> `LOADED_WITH_QUARANTINE`; 2 of 20 -> `HELD`, nothing visible; promote makes it visible; promote on a non-HELD file throws; tampered chunk counts -> `FAILED` and nothing visible; missing chunk result -> `FAILED`; reconcile twice is stable.
- [ ] Commit: `feat(pipeline): reconcile row accounting, hold over-threshold files, promote`.

## Task 13: Seeded fixture generator and golden fixtures

**Files:** `src/fixtures/generate.ts`, `scripts/gen-fixture.ts`, `test/fixtures/*`, `test/core/generate.test.ts`, `package.json` script `"fixtures": "tsx scripts/gen-fixture.ts"`.

- [ ] `generateCustomers({ rows, badRate, seed, onLine? }) -> { csv?: string, labels: Map<number, BadKind> }`. Use a `mulberry32(seed)` PRNG. Header in manifest order. Row `i` (1-based) has `customer_id` `C-<i>` (unique). Valid rows include messy-but-valid values (`"  Ann@Example.COM "`, lowercase `eur`, ` Pro `) so normalisation is exercised, and about 2% of rows wrap the email field in double quotes (valid CSV quoting) so the parser's quote handling is exercised. Bad kinds, each guaranteed invalid: `date_dmy` (`13/01/2026` style with day > 12), `bad_email` (`name@`), `bad_currency` (`EURO`), `bad_amount` (`1,50`), `bad_plan` (`gold`), `missing_id` (empty), `bad_country` (`DEU`), `extra_column` (one extra field). For large files `onLine` streams lines so 1M rows never sit in one string.
- [ ] `scripts/gen-fixture.ts --rows N --bad-rate R --seed S --out path [--labels path]`.
- [ ] Generate and commit: `customers-clean.csv` (200 rows, 0%, seed 1), `customers-3pct.csv` (1,000 rows, 0.03, seed 3), `customers-20pct.csv` (200 rows, 0.20, seed 20), each with `<name>.labels.json` (`{ "badRows": [...], "kinds": {...} }`).
- [ ] Tests: determinism (same seed, same output); every labelled row fails `validateRow` (run normalise + validate on it) and every unlabelled row passes; bad rate within ±1 percentage point at 10,000 rows.
- [ ] Commit: `test: add seeded customer fixture generator and golden inputs`.

## Task 14: In-process driver, readback, golden and fault tests

**Files:** `src/pipeline/driver.ts`, `src/pipeline/readback.ts`, `test/pipeline/driver.golden.test.ts`, `test/pipeline/driver.faults.test.ts`, `test/fixtures/golden/*`.

- [ ] `runIngest(deps, { key }, plan?: FaultPlan) -> IngestRun { sha, status, rowsIn, rowsValid, rowsQuarantined, attempts: Record<number, number>, durationMs }`. Mirror the state machine: `register` -> if `DUPLICATE`/`FAILED` stop -> `split` -> if `FAILED` stop -> read the item list from `manifestKey` -> for each item run `validateTransform` with attempts `0..2`; an error on the last attempt marks the file `FAILED` (`chunk N failed after 3 attempts: <msg>`) and stops -> `reconcile`. Chunks run with a concurrency limit (`plan.concurrency ?? 4`, simple promise pool).
- [ ] `FaultPlan { crashAfterOutput?: Array<number | [chunk, times]>; crashBeforeOutput?: Array<number | [chunk, times]>; }` turns into `FaultHooks` that throw `InjectedFault` on the first `times` (default 1) attempts of those chunks.
- [ ] `readback(objects, dataset) -> { parquetRows, distinctIds, quarantineRows, pendingParquetRows }` lists every visible Parquet part and reads it with `readParquetRows`, lists quarantine JSONL and counts lines (one per record). It never reads the control store.
- [ ] Golden test: for each of the 3 fixtures, ingest with `FsObjectStore` + `FileControlStore` in a temp dir and the fixed clock; compare `{ status, rowsIn, rowsValid, rowsQuarantined, histogram }` to `test/fixtures/golden/<name>.json`, and the quarantine records (sorted by `rowNumber`) to `<name>.quarantine.jsonl`. Also assert the quarantined row numbers equal the fixture's `labels.json` bad rows, and `readback` agrees with the counts. Write goldens when `UPDATE_GOLDEN=1` (via `scripts/it.mjs`-style env passing is not needed; tests read `process.env.UPDATE_GOLDEN`). Generate the goldens once, inspect them, commit them. Expected statuses: clean `LOADED`, 3% `LOADED_WITH_QUARANTINE`, 20% `HELD`.
- [ ] Fault tests (I3, I4): 3% fixture with `chunkRows` 100 (10 chunks) and `crashAfterOutput: [2, [5, 2]]`, `crashBeforeOutput: [7]` -> final counts equal the no-fault run, `readback` shows `parquetRows == distinctIds == rowsValid` and `quarantineRows == rowsQuarantined`; a chunk failing 3 times -> `FAILED` and `readback.parquetRows == 0`; ingest the same bytes twice (second key) -> `DUPLICATE`, readback unchanged; negative control: delete one visible Parquet part and assert the readback check reports `lost > 0` (helper `conservation(run, readback, sourceRows)` used by tests and the benchmark).
- [ ] Commit: `feat(pipeline): add in-process driver with fault injection and file readback`.

## Task 15: Fix, discard, replay, lineage

**Files:** `src/pipeline/fix.ts`, `src/pipeline/replay.ts`, `src/pipeline/lineage-update.ts` (full), `test/pipeline/replay.test.ts`.

- [ ] `loadQuarantine(deps, sha) -> QuarantineRecord[]` (all chunks, sorted by `rowNumber`).
- [ ] `fix(deps, { sha, rowNumber, set: Record<string,string> }) -> { stillInvalid: RowError[] }`: the row must be quarantined in this file (else `StateError`); merge `parsed` + existing approved fix + `set`; write `approvedFixKey` JSON `{ rowNumber, record, approvedAt }`; re-validate and report remaining errors (it is stored either way; replay validates again).
- [ ] `discard(deps, { sha, rowNumbers, reason })`: `putRowStates(..., 'discarded')`; then `refreshParentStatus`.
- [ ] `replay(deps, { sha, onlyFixed }) -> { childKey, childSha, rows } | { nothingToReplay: true, reason }`:
  - Refuse if META has `parentSha` (`StateError`: replay the parent instead).
  - Candidate rows: quarantined rows whose state is `pending` or `requarantined`. With `onlyFixed`, only rows with an approved fix. Fix files under `fixes/proposed/` are never read.
  - Build CSV text with the header in manifest order and each row from the approved fix record or else the original `parsed` record; quote fields with `"` escaping. Child row `i` maps to `parentRows[i-1]`.
  - Compute `childSha = sha256(bytes)`. If an existing edge has this `childSha`, return `nothingToReplay` (same content was already replayed). Otherwise `n = edges.length + 1`, put the object at `replayKey(dataset, sha, n)`, `putReplayEdge`, return. (Locally the CLI then runs `runIngest` on `childKey`; on AWS the S3 event does.)
- [ ] `applyReplayOutcome(deps, childSha)`: if the child META has `parentSha` and the child is visible (`LOADED*`), find the edge, read the child's quarantine row numbers, and set each parent row to `requarantined` (child row quarantined) or `loaded`. Then `refreshParentStatus(parentSha)`: `SUPERSEDED` when `isSuperseded`.
- [ ] Tests (I5, I7): fix a date row and replay `--only-fixed` -> child `LOADED`, parent row `loaded`; an approved fix that is still invalid -> child quarantines it, parent row `requarantined`; a JSON file placed under `fixes/proposed/` is ignored; replaying again with no changes -> `nothingToReplay`; two generations: r1 fixes some rows, r2 fixes the rest -> parent `SUPERSEDED` once the remaining rows are discarded or loaded; every quarantined parent row is in exactly one state after each step (assert with `summarize`); replaying a child throws.
- [ ] Commit: `feat(pipeline): fix, discard and replay quarantined rows with lineage`.

## Task 16: CLI

**Files:** `src/cli/main.ts`, `src/cli/commands.ts`, `test/cli/commands.test.ts`, `package.json` scripts.

- [ ] `commands.ts` exports functions that take local deps (`FsObjectStore` + `FileControlStore` at `root`, real clock) and return printable strings, so tests do not spawn processes. `main.ts` (first line `#!/usr/bin/env node`) wires `commander`: global `--root <dir>` (default `.etl`), `--version` prints `VERSION`.
- [ ] Commands:
  - `etl ingest <file> --dataset <name> [--key-name <name>] [--crash-after-output <list>]`: copy the local file to `rawKey(dataset, basename)`, run `runIngest`, print status, counts and sha.
  - `etl status <sha>`: META fields plus a chunk table (index, rowsIn, valid, quarantined, attempt).
  - `etl files --dataset <name>`.
  - `etl quarantine ls <sha>`: histogram table plus total.
  - `etl quarantine show <sha> --row <n>`: the record as pretty JSON.
  - `etl quarantine fix <sha> --row <n> --set col=value [--set ...]`: prints `approved` and any remaining errors.
  - `etl quarantine discard <sha> --rows 1,2,3 [--reason text]`.
  - `etl replay <sha> [--only-fixed]`: runs `replay` then `runIngest` on the child key; prints the child result and the parent lineage summary.
  - `etl promote <sha>`.
  - `etl curated count --dataset <name>`: `readback` numbers (visible Parquet rows, pending Parquet rows, quarantine rows).
- [ ] Unknown sha -> exit code 1 with `no file with sha <sha>`; errors print one line, no stack trace (stack with `ETL_DEBUG=1`).
- [ ] Tests: in a temp root, ingest the 3% fixture, `quarantine ls` lists the histogram, `show`, `fix` + `replay --only-fixed`, `curated count`, ingest the 20% fixture then `promote`. Assert on key substrings.
- [ ] Run `npm run build` and `node dist/cli/main.js --version` -> `0.1.0`.
- [ ] Commit: `feat(cli): add etl command for ingest, status, quarantine, replay and promote`.

## Task 17: The 30-second demo

**Files:** `src/cli/demo.ts`, `test/cli/demo.test.ts`, `package.json` script `"demo": "npm run build && node dist/cli/main.js demo"`.

- [ ] `etl demo [--rows 50000] [--seed 7] [--root <dir>]`: uses a new `fs.mkdtempSync(os.tmpdir()/etl-demo-)` root unless `--root` is given (a given root must not exist or be empty; never delete a directory the demo did not create). Steps, each printed as a numbered line with its elapsed ms:
  1. Generate the fixture (3% bad).
  2. `etl ingest` it with an injected crash after output on chunk 2 (show `chunk 2: attempt 1 crashed, attempt 2 ok`).
  3. Ingest the same bytes again as `export-copy.csv` -> `DUPLICATE`.
  4. Quarantine histogram (top 5).
  5. Fix every `date_dmy` row by converting `DD/MM/YYYY` to ISO through `fix()` (print one example `etl quarantine fix ...` command and "applied to N rows"), discard rows with an empty `customer_id`.
  6. `replay --only-fixed` -> child status and parent lineage summary.
  7. Final table from `readback` + lineage: source rows, curated rows, still quarantined, discarded, **lost**, **duplicated**, wall time.
- [ ] Test: `demo --rows 2000` in a temp dir finishes, prints `lost 0` and `duplicated 0`, and `DUPLICATE`.
- [ ] Run `npm run demo` once and paste the real output into the ledger line (trimmed).
- [ ] Commit: `feat(cli): add 30-second demo of ingest, crash, duplicate, fix and replay`.

## Task 18: AWS adapters (S3 and DynamoDB)

**Files:** `src/adapters/s3-object-store.ts`, `src/adapters/dynamo-control-store.ts`, `test/adapters/aws-fakes.test.ts`.

- [ ] `S3ObjectStore({ client: S3Client, buckets: Record<BucketName, string> })`: `get` maps `NoSuchKey` to `NotFoundError`; `head` maps `NotFound`/404 to `undefined`; `list` loops on `NextContinuationToken`; `move` = `CopyObject` (`CopySource: <bucket>/<key>` with each path segment `encodeURIComponent`-encoded) then `DeleteObject`, idempotent as in the contract; `getStream` returns `Body` as a Node `Readable`.
- [ ] `DynamoControlStore({ doc: DynamoDBDocumentClient, table })` with the single-table layout from the spec and design doc: `FILE#<sha>/META`, `FILE#<sha>/CHUNK#<00042>`, `DATASET#<name>/FILE#<createdAt>#<sha>` (index item written with META in a `TransactWrite`, or written after a successful conditional put), `DATASET#<name>/DUP#<at>#<sha>`, `FILE#<sha>/REPLAY#<childSha>`, `FILE#<sha>/ROW#<0000000042>`. `createFile` uses `attribute_not_exists(pk)`. `putChunk` uses `attribute_not_exists(pk)` and swallows `ConditionalCheckFailedException`. `recordChunkResult` uses `UpdateCommand` with `ConditionExpression: 'attribute_exists(pk) AND (attribute_not_exists(#a) OR #a <= :a)'` and maps the failure to `'stale'`. Queries paginate on `LastEvaluatedKey`. `putRowStates` uses `BatchWrite` in groups of 25 and retries `UnprocessedItems` (max 5 tries with backoff).
- [ ] `aws-fakes.test.ts`: unit tests with a hand-written fake `send()` that checks the exact command inputs (condition expressions, CopySource encoding of `dataset=customers/ingest_date=2026-10-04/x.parquet`, pagination loops with 2 pages, `UnprocessedItems` retry once). No new dependency (do not add `aws-sdk-client-mock`).
- [ ] Commit: `feat(adapters): add S3 and DynamoDB adapters`.

## Task 19: Integration suite on DynamoDB Local and S3Mock

**Files:** `docker-compose.yml`, `scripts/it.mjs`, `test/integration/aws-adapters.int.test.ts`, `test/integration/aws-pipeline.int.test.ts`, `package.json` scripts.

- [ ] `docker-compose.yml`:
  ```yaml
  name: etl-quarantine
  services:
    ddb:
      image: amazon/dynamodb-local:3.3.1
      container_name: etl-quarantine-ddb
      ports: ["127.0.0.1:5340:8000"]
    s3:
      image: adobe/s3mock:4.11.0
      container_name: etl-quarantine-s3
      ports: ["127.0.0.1:5341:9090"]
  ```
- [ ] Scripts: `"it:up": "docker compose up -d"`, `"it:down": "docker compose down -v"`, `"test:it": "node scripts/it.mjs"` (spawns `npx vitest run test/integration` with `ETL_IT=1` added to the env; `shell: true` on win32).
- [ ] Both files: `describe.skipIf(!process.env.ETL_IT)`. `beforeAll` polls the endpoints (`ETL_IT_DDB_ENDPOINT` default `http://127.0.0.1:5340`, `ETL_IT_S3_ENDPOINT` default `http://127.0.0.1:5341`) for up to 30 s, then creates a uniquely named table (`etlq-<random>`, `PAY_PER_REQUEST`, `pk`/`sk` strings) and four buckets. Credentials `{ accessKeyId: 'test', secretAccessKey: 'test' }`, region `us-east-1`, S3 `forcePathStyle: true`.
- [ ] `aws-adapters.int.test.ts` runs `objectStoreContract` and `controlStoreContract` against the AWS adapters.
- [ ] `aws-pipeline.int.test.ts`: the 3% fixture through `runIngest` on the AWS adapters gives the same counts as its golden JSON; a duplicate delivery gives `DUPLICATE`; `crashAfterOutput: [3]` with `chunkRows: 100` still conserves rows (`readback`).
- [ ] Verify all three paths and record them in the ledger:
  1. `npm test` (no `ETL_IT`): integration tests reported as skipped, exit 0.
  2. `npm run it:up && npm run test:it && npm run it:down`: all pass. Record counts.
  3. `docker ps --filter name=etl-quarantine` is empty afterwards.
- [ ] Commit: `test(integration): run adapter contracts and pipeline on DynamoDB Local and S3Mock`.

## Task 20: Lambda handlers and bundle

**Files:** `src/handlers/env.ts`, `src/handlers/register.ts`, `src/handlers/split.ts`, `src/handlers/validate-transform.ts`, `src/handlers/reconcile.ts`, `scripts/bundle.mjs`, `test/handlers/handlers.test.ts`, `package.json` script `"bundle": "node scripts/bundle.mjs"`.

- [ ] `env.ts`: `depsFromEnv()` reads `RAW_BUCKET`, `STAGING_BUCKET`, `CURATED_BUCKET`, `QUARANTINE_BUCKET`, `CONTROL_TABLE` (throws `missing env RAW_BUCKET` etc.), builds the AWS adapters once per container. `ETL_FAULT_AFTER_OUTPUT_CHUNKS` (comma list) enables the after-output fault on attempt 0 for those chunks (used only for drills; documented).
- [ ] Each handler module exports `createHandler(makeDeps)` and `handler = createHandler(depsFromEnv)`. Payloads: register `{ key }`; split `{ sha }` (from `$.file.sha`); validate-transform `{ sha, chunk: { sha, index, key, rows }, attempt }`; reconcile `{ sha }`. Handlers return the step result unchanged.
- [ ] `scripts/bundle.mjs`: esbuild for the 4 entry points -> `build/lambda/<name>/index.mjs`, `bundle: true`, `platform: 'node'`, `target: 'node24'`, `format: 'esm'`, `sourcemap: true`, `banner: { js: "import { createRequire } from 'module'; const require = createRequire(import.meta.url);" }`. Print each bundle's size.
- [ ] Tests: each handler with in-memory deps returns the expected shape; `depsFromEnv` error message for a missing variable; after `npm run bundle`, `node -e "import('./build/lambda/register/index.mjs').then(m => console.log(typeof m.handler))"` prints `function` (record bundle sizes in the ledger).
- [ ] Commit: `feat(handlers): add Lambda handlers and esbuild bundles`.

## Task 21: StorageStack

**Files:** `infra/lib/storage-stack.ts`, `infra/test/storage.test.ts`.

- [ ] Four buckets: all `BlockPublicAccess.BLOCK_ALL`, `encryption: S3_MANAGED`, `enforceSSL: true`. Raw: `eventBridgeEnabled: true`, lifecycle expire 30 days. Staging: expire 7 days. Quarantine: expire 90 days. Curated: no expiry, `RemovalPolicy.RETAIN`. Control table: `TableV2`, `pk`/`sk` strings, on-demand, point-in-time recovery on, `RemovalPolicy.RETAIN`. Expose buckets and table as public readonly fields.
- [ ] Tests (I11) with `Template.fromStack`: 4 buckets with SSE and public access block; raw bucket has `NotificationConfiguration.EventBridgeConfiguration`; lifecycle days 30/7/90; a bucket policy denying `aws:SecureTransport: false` on each; the table keys, billing mode and PITR.
- [ ] Commit: `feat(infra): add storage stack with encrypted buckets, lifecycle and control table`.

## Task 22: PipelineStack, app and synth

**Files:** `infra/lib/pipeline-stack.ts`, `infra/lib/nag-suppressions.ts`, `infra/bin/app.ts`, `cdk.json`, `infra/test/pipeline.test.ts`, `infra/test/nag.test.ts`, `package.json` script `"synth": "npm run bundle && cdk synth --quiet"`.

- [ ] `PipelineStack` props: `storage` refs and `lambdaCodeDir` (default `build/lambda`). Four `lambda.Function`s (`NODEJS_24_X`, `ARM_64`, `handler: 'index.handler'`, `Code.fromAsset(join(codeDir, name))`, env vars from Task 20, timeouts: register 5 min, split 15 min, validate-transform 2 min with 1024 MB, reconcile 5 min, log retention 1 month).
- [ ] IAM with explicit `PolicyStatement`s only (no `grant*` helpers, so no wildcard actions): register: raw `s3:GetObject`; table `dynamodb:GetItem`, `PutItem`, `UpdateItem`, `Query`. split: raw `s3:GetObject`, staging `s3:PutObject`, table same set. validate-transform: staging `s3:GetObject`, curated `s3:PutObject` on `arn/_pending/*` only, quarantine `s3:PutObject`, table same set. reconcile: curated `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` (bucket ARN), quarantine `s3:GetObject` + `s3:ListBucket`, table same set plus `dynamodb:BatchWriteItem`.
- [ ] State machine (Standard) exactly as prototyped: `Register` (payload `{ key: $.detail.object.key }`, `resultPath: '$.file'`) -> `Choice` on `$.file.status` (`DUPLICATE` -> `Succeed`, `FAILED` -> `Fail`) -> `Split` (payload `{ sha: $.file.sha }`, `resultPath: '$.split'`) -> `Choice` (`FAILED` -> `Fail`) -> `DistributedMap` (`S3JsonItemReader` on staging with key `$.split.manifestKey`, `maxConcurrency: 20`, `toleratedFailurePercentage: 0`, `resultPath: DISCARD`) whose processor is `ValidateTransform` (payload `{ sha: $.sha, chunk: $, attempt: $$.State.RetryCount }`, `retryOnServiceExceptions: false`, `addRetry({ errors: ['States.ALL'], maxAttempts: 2, interval: 2s, backoffRate: 2 })`) -> `Reconcile` (payload `{ sha: $.file.sha }`). Grant the state machine role read on the staging bucket for the item reader.
- [ ] EventBridge rule: `source: ['aws.s3']`, `detailType: ['Object Created']`, `detail: { bucket: { name: [raw.bucketName] }, object: { key: Match.wildcard('dataset=*.csv'), size: Match.greaterThan(0) } }`, target the state machine.
- [ ] `infra/bin/app.ts`: `StorageStack` + `PipelineStack` named `EtlQuarantineStorage` / `EtlQuarantinePipeline`, env-agnostic. `Aspects.of(app).add(new AwsSolutionsChecks())` plus suppressions from `nag-suppressions.ts`, each with a written reason. `cdk.json`: `{ "app": "npx tsx infra/bin/app.ts", "output": "cdk.out" }`.
- [ ] Tests: definition contains `DISTRIBUTED`, `ItemReader`, `MaxConcurrency: 20`, `"attempt.$":"$$.State.RetryCount"`, `MaxAttempts: 2`; the rule pattern (wildcard and numeric); every Lambda role statement has no action containing `*`; validate-transform has no `s3:PutObject` on the raw bucket and its curated `PutObject` resource ends in `/_pending/*`; 4 functions on `nodejs24.x`. `nag.test.ts`: synthesize with `AwsSolutionsChecks` and assert zero unsuppressed errors. Tests pass a temp `lambdaCodeDir` with stub `index.mjs` files so they do not need `npm run bundle`.
- [ ] Run `npm run synth` - exit 0, `cdk.out/EtlQuarantinePipeline.template.json` exists. Record resource counts per stack in the ledger.
- [ ] Commit: `feat(infra): add pipeline stack with EventBridge, Distributed Map state machine and least-privilege IAM`.

## Task 23: Headline benchmark

**Files:** `bench/bench.ts`, `bench/results.json`, `bench/RESULTS.md`, `package.json` script `"bench": "tsx bench/bench.ts"`.

- [ ] Follow the spec's "Benchmark protocol". CLI args: `--rows` (default 1,000,000), `--sweep-runs` (default 20), `--sweep-rows` (default 100,000), `--out` (default `bench`). Data goes to `bench/data/` (gitignored); temp roots under `os.tmpdir()` are removed at the end.
- [ ] Headline run: seed 42, bad rate 0.03, `chunkRows` 5,000, concurrency 4. Pick ~5% of chunks (seeded) for `crashAfterOutput`, ~2% for `crashBeforeOutput`, one chunk with `[chunk, 2]` (two crashes), then ingest the same bytes a second time. Time only the ingest (not generation). Record: `rowsIn`, `rowsValid`, `rowsQuarantined`, status, `chunks`, `injectedCrashes`, `retries`, duplicate status, readback numbers, `lost`, `duplicated`, `labelsMatch` (quarantined row numbers equal the generator's labels), `ingestSeconds`, `rowsPerSecond`, peak RSS (sample `process.memoryUsage().rss` every 100 ms).
- [ ] Sweep: for seeds `1..N`, a `--sweep-rows` file with random crash sets (seeded, including some double crashes) and a duplicate delivery; record per-run `lost`, `duplicated`, `labelsMatch`, and `conservedRuns / totalRuns`.
- [ ] Write `bench/results.json` (also `node`, `os`, `cpu` model and count, `timestamp`, git commit if available) and `bench/RESULTS.md` (a short table generated from the JSON, plus the exact command).
- [ ] Run `npm run bench 2>&1 | tail -20`. Paste the headline numbers into the ledger exactly as printed. If any run is not conserved, **stop**: that is a bug (use superpowers:systematic-debugging), not a number to publish.
- [ ] Commit (results included): `perf: add 1M-row fault-injection benchmark with measured results`.

## Task 24: Pack smoke, CI and the gates script

**Files:** `scripts/pack-smoke.mjs`, `.github/workflows/ci.yml`, `package.json` scripts `"smoke:pack"`, `"gates"`.

- [ ] `scripts/pack-smoke.mjs`: `npm pack --pack-destination <tmp>`; in a fresh temp dir `npm init -y` and `npm install <tarball>`; run `npx --no-install etl --version` (expect `0.1.0`) and `npx --no-install etl demo --rows 2000 --root <tmp>/demo` (expect exit 0 and `lost 0`); print `PACK SMOKE OK` plus the tarball size; clean up the temp dirs. Use `shell: true` on win32.
- [ ] `"gates": "npm run typecheck && npm test && npm run build && npm run synth && npm run smoke:pack"`.
- [ ] `ci.yml` (on push and pull_request): job `gates` (ubuntu-latest, `actions/setup-node@v4` node 24 with npm cache, `npm ci`, `npm run gates`, then `npm run bench -- --rows 100000 --sweep-runs 3 --sweep-rows 20000 --out bench/ci` as a smoke, not committed). Job `integration` with service containers `amazon/dynamodb-local:3.3.1` (ports `5340:8000`) and `adobe/s3mock:4.11.0` (ports `5341:9090`), `npm ci`, `npm run test:it`.
- [ ] Validate the workflow: `docker run --rm -v "<repo>:/repo" -w /repo rhysd/actionlint:1.7.7` (if that tag is unavailable, use the newest tag that exists and note it) - expect no findings. Remove the container (the `--rm` does it).
- [ ] Run `npm run gates 2>&1 | tail -30` - expect exit 0 and `PACK SMOKE OK`.
- [ ] Commit: `ci: add gates workflow, pack smoke and emulator integration job`.

## Task 25: README and handoff

**Files:** `README.md`, `docs/handoff.md`.

- [ ] README, in this order:
  1. Title and a one-line headline built **only** from `bench/results.json` (for example: "1,000,000 rows, N injected chunk crashes and a duplicate delivery: 0 lost, 0 duplicated, X rows/s on one machine", with the real values). Link `bench/RESULTS.md`.
  2. "30 seconds": `npx etl demo` (after `npm ci && npm run build`, or from the packed tarball) and the real demo transcript from Task 17 (trimmed, unedited numbers).
  3. Mermaid architecture diagram (S3 raw -> EventBridge -> Step Functions: Register -> Split -> Distributed Map(ValidateTransform) -> Reconcile; curated `_pending` -> visible; quarantine -> fix -> replay -> raw `_replay/`).
  4. Invariants table (I1-I8, I11) with the proving test file for each.
  5. CLI reference.
  6. Testing: real counts from the last `npm test`, `npm run test:it`, `npm run synth`.
  7. "Decisions" linking ADRs 0001-0007, one line each with what was given up.
  8. "Known limits": no deployment (driver stands in for Step Functions), no Athena/Glue, no LLM suggester, file control store is single-writer, append-only, no cost number.
- [ ] `docs/handoff.md`: create with a `2026-10-04, Claude (Sonnet builder), branch main` entry: what changed, what is left, how to verify (the gate commands).
- [ ] Final verification, all from a clean tree: `npm ci`, `npm run gates`, `npm run it:up && npm run test:it && npm run it:down`, `docker ps --filter name=etl-quarantine` empty, `git status --short` empty after the commit. Record the real outputs in the ledger.
- [ ] Commit: `docs: add README with measured headline, demo transcript and handoff`.

---

## Gates (the reviewer re-runs these)

1. `npm ci` - exit 0.
2. `npm run typecheck` - exit 0.
3. `npm test` - exit 0; integration files skipped without `ETL_IT`.
4. `npm run build` - exit 0; `node dist/cli/main.js --version` prints `0.1.0`.
5. `npm run synth` - exit 0; `cdk.out/EtlQuarantineStorage.template.json` and `cdk.out/EtlQuarantinePipeline.template.json` exist; nag test passes.
6. `npm run smoke:pack` - prints `PACK SMOKE OK`.
7. `npm run it:up && npm run test:it && npm run it:down` - all integration tests pass; no `etl-quarantine-*` container left running.
8. `npm run bench` - `bench/results.json` shows `lost: 0`, `duplicated: 0`, `labelsMatch: true` for the headline run and `conservedRuns == totalRuns` for the sweep. README numbers match the file.
