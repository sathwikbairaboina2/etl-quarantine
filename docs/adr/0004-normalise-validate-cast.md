# ADR 0004: Normalise, then validate strings with Ajv, then cast only valid rows

Status: accepted, 2026-10-04

## Context

CSV gives strings. The schema must be a contract a producer can reuse (so JSON Schema, not Zod). Validation alone must decide what reaches curated data (I2). If casting ran before validation, a failed cast would be a second, hidden validator.

## Decision

- Order per row: `normalize` (manifest ops `trim`, `lowercase`, `uppercase`, `emptyToNull`), then Ajv 2020-12 validation of the normalised strings, then `cast` to Parquet types.
- The schema constrains every string's shape (`pattern`, `format`, `enum`), so `cast` cannot fail for a row that passed. If it ever does, `cast` throws an `InvariantError` and the chunk fails; the row is never dropped silently.
- The validator returns a branded `ValidRow`. The Parquet writer only accepts `ValidRow[]`, so the type system stops an unvalidated row from reaching it.
- Ajv options: `allErrors: true`, `strict: true`, `coerceTypes: false`, with `ajv-formats` for `email` and `date`. Validators are compiled once per schema id and cached.
- Quarantine errors keep Ajv's `instancePath`, `keyword` and `message`. A wrong column count gives `{ instancePath: "", keyword: "columnCount" }`.

## Consequences

- One place decides validity, and every quarantine record shows the exact Ajv errors.
- What we gave up: typed JSON values in the schema (numbers, booleans). JSONL producers that send real numbers later need a second schema variant or a coercion step in front of the validator.
