# etl-quarantine v0.1: spec (2026-10-04)

Planner: Claude Opus. Builder: Sonnet. Design source: `C:\Users\sathwik\projects\taskarinchu\docs\devdocs\etl-quarantine.md` (read it for the full design; this spec records what v0.1 builds and every place it deliberately differs).

## One-line pitch

Drop a CSV into the raw bucket and every row is validated against a versioned JSON Schema: good rows land as Parquet, bad rows land in a quarantine you can inspect, fix and replay with one command, and the pipeline proves that no row is lost or loaded twice, even when chunks crash and files are delivered twice.

## Portfolio bar (from the 2026-10-03 shortlist) and how v0.1 meets it

| Bar | v0.1 answer |
|---|---|
| 30-second wow | `npx etl demo` (also `npm run demo`): generates a seeded 50k-row export with ~3% bad rows, ingests it with an injected chunk crash, re-delivers the same file (DUPLICATE), prints the quarantine error histogram, fixes rows with one command, replays, and ends with a row-accounting table that reads Parquet and quarantine files back from disk. Runs offline in seconds. A recorded transcript goes in the README. |
| Measured headline number | `npm run bench` writes `bench/results.json` and `bench/RESULTS.md`: 1,000,000 rows with 3% seeded bad rows, injected chunk crashes and a duplicate delivery, then counts rows **read back from the Parquet and quarantine files** (not from the control table). Headline: lost rows, duplicated rows, rows/s. Plus a fault-injection sweep (N seeds) reporting how many runs conserved every row. Only numbers from that file go in the README. |
| Something installable | The npm package `etl-quarantine` with an `etl` bin. `npm run smoke:pack` packs it, installs the tarball into a temp dir and runs `etl --version` and `etl demo --rows 2000`. |
| Honest ADRs | `docs/adr/0001`-`0007`, each with "what we gave up". |
| CI with tests | `.github/workflows/ci.yml`: typecheck, unit/property/golden tests, build, Lambda bundle, `cdk synth`, pack smoke, and a DynamoDB Local + S3Mock integration job (service containers, no tokens). |

## Environment facts that shape v0.1

- No LocalStack token exists, so nothing deploys to LocalStack or AWS. AWS work is proven by CDK assertion tests and `cdk synth`. (ADR 0002)
- Free, token-less emulators were prototyped on 2026-10-04 and work: `amazon/dynamodb-local:3.3.1` (conditional writes) and `adobe/s3mock:4.11.0` (path-style S3). They back an opt-in integration suite (`ETL_IT=1`) that runs the real AWS SDK adapters through the in-process driver. (ADR 0006)
- Host ports for this repo: 5340-5349 only. Integration containers: `etl-quarantine-ddb` on `127.0.0.1:5340`, `etl-quarantine-s3` on `127.0.0.1:5341`.
- Node 24 is on the host (v24.18.0). npm 11.

## Scope

### In v0.1

1. **Pure core** (`src/core/`, no AWS SDK imports): dataset manifest loading and validation, CSV chunk parsing (BOM, CRLF, quoted newlines, ragged rows), normalisers, Ajv 2020-12 validation with a branded `ValidRow`, typed casting, row accounting, deterministic key derivation, input limits, lineage partition.
2. **Ports and adapters** (`src/ports.ts`, `src/adapters/`): `ObjectStore` and `ControlStore` interfaces; `FsObjectStore` and `FileControlStore` for local runs; `S3ObjectStore` and `DynamoControlStore` for Lambda. One shared contract test suite runs against every adapter.
3. **Pipeline steps** (`src/pipeline/`): `register`, `split`, `validateTransform`, `reconcile`, plus operator actions `fix`, `discard`, `replay`, `promote`. Each step is a plain async function over ports. Lambda handlers and the local driver call the same functions with the same payload shapes.
4. **In-process driver** (`src/pipeline/driver.ts`): mirrors the state machine (Register → Duplicate? → Split → Map(chunks, retry 3x) → Reconcile), with fault injection used by tests, the demo and the benchmark.
5. **CLI** `etl` (local mode, `--root <dir>`, default `./.etl`): `ingest`, `status`, `files`, `quarantine ls|show|fix|discard`, `replay`, `promote`, `curated count`, `demo`.
6. **Lambda handlers** (`src/handlers/`) and an esbuild bundle (`build/lambda/<fn>/index.mjs`).
7. **CDK** (`infra/`): `StorageStack` (4 buckets + control table) and `PipelineStack` (5 Lambdas, EventBridge rule, Standard state machine with a Distributed Map). Assertion tests for I4-relevant retry config, I5-style IAM scoping, I11 encryption and lifecycle, and the EventBridge pattern.
8. **Fixtures and benchmark**: seeded generator (`scripts/gen-fixture.ts`), golden fixtures, `bench/bench.ts`.
9. Docs: README, `docs/DEVDOCS.md`, ADRs, `docs/handoff.md`.

### Deferred (v0.2+), stated in the README "Known limits"

- Glue Data Catalog, Athena workgroup, partition projection (I10). v0.1 offers `etl curated count` (reads Parquet with hyparquet) instead.
- LLM fix suggester (`fixes/proposed/`). v0.1 has human fixes only (`fixes/approved/`), so I5 holds trivially: nothing but an operator command writes an approved fix.
- JSONL input, `etl schema diff` and its CI gate (I9), SNS/Slack alerts, EventBridge archive.
- Upserts or primary-key dedupe across files (append-only).
- Real deploys (LocalStack or AWS) and cost per million rows. No cost number is published.

## Differences from the design doc (rulings)

- **Keys inside each bucket omit the bucket name.** Raw: `dataset=<name>/<file>.csv`; replays: `dataset=<name>/_replay/<parentSha>-r<n>.csv`. Staging: `<sha>/chunk-00042.jsonl` plus `<sha>/chunks.json` (the Map item list). Curated: `_pending/dataset=<name>/ingest_date=<d>/<sha>-00042.parquet` until reconcile, then `dataset=<name>/ingest_date=<d>/<sha>-00042.parquet`. Quarantine: `dataset=<name>/<sha>/chunk-00042.jsonl`, fixes at `dataset=<name>/<sha>/fixes/approved/row-<n>.json`.
- **All chunk output is written under `_pending/` first** and `reconcile` moves it to the visible prefix only for `LOADED` or `LOADED_WITH_QUARANTINE`. `HELD` and `FAILED` output stays in `_pending/`. This makes "nothing becomes visible unless rows are conserved" true by construction (ADR 0005).
- **Normalise → validate → cast.** CSV yields strings. Manifest `normalize` ops (`trim`, `lowercase`, `uppercase`, `emptyToNull`) run first, Ajv validates the normalised strings (patterns, formats, enums), and only a `ValidRow` is cast to typed Parquet columns. A cast cannot fail on a valid row because the schema constrains the string shape (ADR 0004).
- **Row identity** is `rowNumber`: the 1-based data-record index in the source file (header excluded). Quarantine records also keep the raw CSV text of the record.
- **Malformed CSV structure:** a row with the wrong column count is quarantined with `{ instancePath: "", keyword: "columnCount" }`. An unrecoverable parse error (for example an unclosed quote) or an over-limit row fails the whole file as `FAILED` with the parser message, before anything is visible.
- **Limits** are constants with test overrides: max file 5 GiB (checked at register from object size), max row 1 MiB and max 200 columns (checked at split). Tests exercise limit and limit+1 with small overrides.
- **Lineage (I7) in v0.1:** every quarantined row of a parent file ends in exactly one state: `pending`, `loaded` (validated and loaded in a replay child), `requarantined` (failed again in a child), or `discarded`. The parent becomes `SUPERSEDED` when no row is `pending` or `requarantined`. A child replay file lists its parent row numbers in the `REPLAY#<childSha>` edge.

## Invariants v0.1 proves (numbering from the design doc)

| # | Invariant | Proving tests in v0.1 |
|---|---|---|
| I1 | `rowsIn == rowsValid + rowsQuarantined` per chunk and per file, otherwise `FAILED` and nothing visible | `test/core/accounting.property.test.ts` (fast-check); `test/pipeline/reconcile.test.ts`; golden fixtures; benchmark readback |
| I2 | No row reaches Parquet without passing the pinned schema | `test/core/validate.test.ts` malformed-row table; `test/pipeline/validate-transform.test.ts` reads the Parquet back and asserts none of the bad rows are present |
| I3 | Same content is loaded at most once | `test/pipeline/register.test.ts`; driver duplicate-delivery test; integration suite on DynamoDB Local |
| I4 | Chunk retries are idempotent | `test/pipeline/driver.faults.test.ts` (crash after Parquet write, crash before count write); attempt-conditional count write in the ControlStore contract suite |
| I5 | Only approved fixes are replayed and replays are fully revalidated | `test/pipeline/replay.test.ts` (an approved fix that is still invalid is re-quarantined, a file in `fixes/proposed/` is ignored) |
| I6 | Over-threshold files are invisible until promoted | `test/pipeline/reconcile.test.ts` and golden 20%-bad fixture: status `HELD`, `etl curated count` = 0, then `promote` makes it `rowsValid` |
| I7 | Every quarantined row is accounted for exactly once across replays | `test/core/lineage.property.test.ts` and a two-generation replay test |
| I8 | Input limits | `test/core/limits.test.ts` boundary cases |
| I11 | Encryption and retention | `infra/test/storage.test.ts`: SSE, BlockPublicAccess, enforceSSL, lifecycle 90 days on quarantine, 30 days on raw, 7 days on staging |

## Interfaces (the builder must keep these names)

```ts
// src/ports.ts
export type BucketName = 'raw' | 'staging' | 'curated' | 'quarantine';
export interface ObjectStore {
  put(bucket: BucketName, key: string, body: Uint8Array | string): Promise<void>;
  get(bucket: BucketName, key: string): Promise<Uint8Array>;           // throws NotFoundError
  getStream(bucket: BucketName, key: string): Promise<NodeJS.ReadableStream>;
  head(bucket: BucketName, key: string): Promise<{ size: number } | undefined>;
  list(bucket: BucketName, prefix: string): Promise<string[]>;          // sorted, all pages
  delete(bucket: BucketName, key: string): Promise<void>;               // no error if missing
  move(bucket: BucketName, fromKey: string, toKey: string): Promise<void>; // idempotent
}
export interface ControlStore {
  createFile(meta: FileMeta): Promise<'created' | 'exists'>;            // conditional on FILE#sha META absent
  getFile(sha: string): Promise<FileMeta | undefined>;
  updateFile(sha: string, patch: Partial<FileMeta>): Promise<void>;
  listFiles(dataset: string): Promise<FileMeta[]>;                      // newest first
  putChunk(sha: string, chunk: ChunkRecord): Promise<void>;             // split result, rowsIn
  recordChunkResult(sha: string, index: number, r: ChunkResult): Promise<'applied' | 'stale'>; // set, never add; applies only if r.attempt >= stored attempt
  listChunks(sha: string): Promise<ChunkRecord[]>;                      // by index
  putReplayEdge(parentSha: string, edge: ReplayEdge): Promise<void>;
  listReplayEdges(parentSha: string): Promise<ReplayEdge[]>;
  putRowStates(parentSha: string, entries: Array<[number, RowState]>): Promise<void>; // batch
  listRowStates(parentSha: string): Promise<Map<number, RowState>>;
  recordDuplicate(dataset: string, sha: string, key: string, at: string): Promise<void>;
}
```

Status values: `REGISTERED`, `SPLIT`, `PROCESSING`, `LOADED`, `LOADED_WITH_QUARANTINE`, `HELD`, `FAILED`, `DUPLICATE` (on the run result, not stored over the original), `SUPERSEDED`.

Duplicate handling: the content sha is the file id. A second delivery of the same bytes returns `{ status: 'DUPLICATE', sha, originalKey }` and writes a `DATASET#<name> / DUP#<ts>#<sha>` audit item (FileControlStore: an entry in `duplicates`). It never touches the original META.

## Dataset `customers` (v1)

CSV header: `customer_id,email,country,currency,amount,contract_start,plan`.

| Column | Normalise | Schema (strings) | Parquet type |
|---|---|---|---|
| customer_id | trim | required, `^C-[0-9]{1,10}$` | STRING, required |
| email | trim, lowercase, emptyToNull | `["string","null"]`, format `email` | STRING, optional |
| country | trim, uppercase | required, `^[A-Z]{2}$` | STRING, required |
| currency | trim, uppercase | required, enum `EUR, USD, GBP` | STRING, required |
| amount | trim | required, `^-?[0-9]{1,12}(\.[0-9]{1,2})?$` | `amount_cents` INT64, required |
| contract_start | trim | required, format `date` | DATE (INT32), required |
| plan | trim, lowercase | required, enum `basic, pro, enterprise` | STRING, required |

`additionalProperties: false`. Manifest: `quarantineThreshold: 0.05`, `chunkRows: 5000`, `partitionBy: "ingest_date"`.

## Fault injection (driver and benchmark)

`FaultPlan` options: `crashAfterOutputWrite: number[]` (chunk indexes whose first attempt throws after writing Parquet and quarantine but before recording counts), `crashBeforeOutputWrite: number[]`, `duplicateDelivery: boolean` (ingest the same bytes twice), `seed`. Retries: max 3 attempts per chunk, like the state machine's `Retry`.

## Benchmark protocol (headline)

`npm run bench` (on built `dist/`):
1. Generate `bench/data/customers-1m.csv` with seed 42 and 3% bad rows (gitignored).
2. Ingest through the driver with `FsObjectStore` + `FileControlStore` in a temp root, with crashes injected into ~5% of chunks (seeded) and a duplicate delivery.
3. Read back: count rows across all visible Parquet files with hyparquet, count distinct `customer_id` among them, count quarantine records. `lost = rowsInSource - (parquetRows + quarantineRows)`, `duplicated = parquetRows - distinctIds` (fixture ids are unique per row).
4. Fault sweep: 20 seeds on a 100k-row file, each with random crash points; report conserved runs / total.
5. Write `bench/results.json` (machine, node version, timestamp, every number) and `bench/RESULTS.md`. The README quotes these.

## Out-of-scope guardrails for the builder

No Glue, Athena, Bedrock, Ollama or SNS code. No LocalStack. No real AWS calls. No global installs. No secrets.
