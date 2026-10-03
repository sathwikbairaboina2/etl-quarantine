# ADR 0005: Exactly-once loading from content hashes, deterministic keys and a pending prefix

Status: accepted, 2026-10-04

## Context

EventBridge can deliver an S3 event more than once, producers re-upload the same export, and Map iterations retry after a crash. The pipeline must still load each row at most once and never lose one (I1, I3, I4, I6).

## Decision

- **File identity is `sha256(content)`**, streamed from the object store. `register` does a conditional create of `FILE#<sha> / META`. A second delivery returns `DUPLICATE` and writes only an audit item.
- **Chunk outputs have deterministic keys** (`<sha>-<chunk>.parquet`, `chunk-<chunk>.jsonl`), so a retried chunk overwrites its own output.
- **Chunk counts are set, never added**, and only when the attempt number is at least the stored one (DynamoDB `ConditionExpression: attribute_not_exists(#a) OR #a <= :a`; the file store applies the same rule).
- **All chunk output goes to `curated/_pending/...` first.** `reconcile` checks `rowsIn == rowsValid + rowsQuarantined` per chunk and per file, then moves the parts to the visible prefix only for `LOADED` or `LOADED_WITH_QUARANTINE`. `HELD` (quarantine ratio over the threshold) and `FAILED` output stays in `_pending/`. `etl promote` moves a `HELD` file's parts later. Moves are idempotent: if the source is gone and the target exists, the move counts as done.

## Consequences

- Re-delivery, retries and crashes between "wrote output" and "recorded counts" cannot create duplicate rows. The fault-injection tests and the benchmark check this by reading the files back, not by trusting the counters.
- What we gave up:
  - Hashing a large file costs a full read before any work starts. The design's "short-circuit on `versionId`" is deferred.
  - Copy-then-delete moves double the S3 write requests for curated data.
  - Two different files that contain the same row are both loaded (append-only, no primary-key dedupe).
