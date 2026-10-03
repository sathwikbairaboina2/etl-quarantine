# ADR 0006: Local file stores, and an opt-in integration suite on DynamoDB Local and S3Mock

Status: accepted, 2026-10-04

## Context

The CLI, the demo and the benchmark need state that survives between commands, with no Docker. The Lambda adapters (S3, DynamoDB) still need proof that conditional writes and pagination behave as the ports promise. LocalStack is out (ADR 0002).

## Decision

- **Local mode:** `FsObjectStore` maps `bucket/key` to `<root>/<bucket>/<key>`. `FileControlStore` keeps the control table in `<root>/control.json`, loads it once, and rewrites it atomically (temp file, then rename) after every mutation. It assumes a single writer process.
- **One contract suite** (`test/adapters/contract.ts`) defines port behaviour: conditional create, attempt-conditional chunk results, list ordering, pagination, idempotent move and delete. It runs against the in-memory stores and the file stores always, and against the AWS SDK adapters when `ETL_IT=1`.
- **Emulators** (prototyped 2026-10-04, no tokens needed): `amazon/dynamodb-local:3.3.1` on `127.0.0.1:5340` and `adobe/s3mock:4.11.0` on `127.0.0.1:5341`, from `docker-compose.yml` with container names `etl-quarantine-ddb` and `etl-quarantine-s3`. CI runs the same images as service containers.
- Without `ETL_IT=1` the integration files are skipped with `describe.skipIf`, so `npm test` needs no Docker.

## Consequences

- The DynamoDB condition expressions and S3 pagination are tested against real protocol implementations, not only fakes.
- What we gave up: the file control store is unsafe for two concurrent CLI processes and rewrites the whole JSON file per mutation (fine for thousands of items, not millions). The emulators are not AWS: throttling, consistency and some error shapes differ.
