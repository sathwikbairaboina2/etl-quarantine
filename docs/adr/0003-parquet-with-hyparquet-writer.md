# ADR 0003: Write Parquet with hyparquet-writer (pure JS)

Status: accepted, 2026-10-04

## Context

Curated rows must be Parquet so Athena (later) and DuckDB can scan them cheaply. The writer runs inside a Node Lambda and in the local CLI on Windows. Options: `pyarrow` in a Python Lambda (a second runtime, a large layer), `@dsnp/parquetjs` (streams, older code base), `parquet-wasm` (needs Arrow tables and a WASM binary), and `hyparquet-writer` (pure JS, one dependency, writes from column arrays).

## Decision

- Use `hyparquet-writer@0.16.10` to write and `hyparquet@1.31.1` to read back in tests, the CLI and the benchmark.
- One Parquet file per chunk (default 5,000 rows), built in memory with `parquetWriteBuffer` and an explicit schema: strings as `BYTE_ARRAY` + `UTF8`, `amount_cents` as `INT64` (BigInt values), `contract_start` as `INT32` + `DATE` (Date values), with `REQUIRED` or `OPTIONAL` taken from the manifest.
- Prototype on 2026-10-04: 5,000 rows with 4 columns wrote in about 50 ms (71 KB). A `DATE` column from an explicit schema round-trips through hyparquet. A `null` in a `REQUIRED` column throws, which the cast step must never produce.

## Consequences

- No native modules, so the Lambda bundle is one esbuild file and Windows needs no build tools.
- What we gave up: streaming writes (a chunk is held in memory, which bounds the chunk size), the maturity of Arrow-based writers, and tuning such as dictionary and statistics options we have not checked. The 1M-row benchmark measures the throughput we actually get.
